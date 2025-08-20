package com.guno.dataimport.repository;

import com.guno.dataimport.entity.OrderStatus;
import com.guno.dataimport.util.CsvFormatter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * OrderStatus Repository - JDBC operations with COPY FROM optimization
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class OrderStatusRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final String UPSERT_SQL = """
        INSERT INTO tbl_order_status (
            status_key, order_id, transition_date_key, transition_timestamp,
            duration_in_previous_status_hours, transition_reason, transition_trigger,
            changed_by, is_on_time_transition, is_expected_transition, history_key
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (status_key, order_id) DO UPDATE SET
            transition_timestamp = EXCLUDED.transition_timestamp,
            duration_in_previous_status_hours = EXCLUDED.duration_in_previous_status_hours,
            transition_reason = EXCLUDED.transition_reason,
            transition_trigger = EXCLUDED.transition_trigger,
            changed_by = EXCLUDED.changed_by,
            is_on_time_transition = EXCLUDED.is_on_time_transition,
            is_expected_transition = EXCLUDED.is_expected_transition
        """;

    private static final String COPY_SQL = """
        COPY tbl_order_status (
            status_key, order_id, transition_date_key, transition_timestamp,
            duration_in_previous_status_hours, transition_reason, transition_trigger,
            changed_by, is_on_time_transition, is_expected_transition, history_key
        ) FROM STDIN WITH (FORMAT CSV, DELIMITER ',')
        """;

    /**
     * OPTIMIZED: Bulk upsert with COPY FROM fallback
     */
    public int bulkUpsert(List<OrderStatus> orderStatuses) {
        if (orderStatuses == null || orderStatuses.isEmpty()) return 0;

        try {
            return bulkUpsertWithPreDelete(orderStatuses);
        } catch (Exception e) {
            log.warn("COPY FROM failed, using batch upsert: {}", e.getMessage());
            return executeBatchUpsert(orderStatuses);
        }
    }

    public List<OrderStatus> findByOrderIds(Set<String> orderIds) {
        if (orderIds == null || orderIds.isEmpty()) return List.of();

        String sql = "SELECT * FROM tbl_order_status WHERE order_id = ANY(?) ORDER BY transition_timestamp DESC";
        return jdbcTemplate.query(sql, orderStatusRowMapper(), orderIds.toArray(new String[0]));
    }

    public List<OrderStatus> findByOrderId(String orderId) {
        String sql = "SELECT * FROM tbl_order_status WHERE order_id = ? ORDER BY transition_timestamp DESC";
        return jdbcTemplate.query(sql, orderStatusRowMapper(), orderId);
    }

    public OrderStatus findCurrentStatusByOrderId(String orderId) {
        String sql = """
            SELECT * FROM tbl_order_status 
            WHERE order_id = ? 
            ORDER BY transition_timestamp DESC 
            LIMIT 1
            """;
        List<OrderStatus> results = jdbcTemplate.query(sql, orderStatusRowMapper(), orderId);
        return results.isEmpty() ? null : results.get(0);
    }

    public OrderStatus findByStatusKeyAndOrderId(Long statusKey, String orderId) {
        String sql = "SELECT * FROM tbl_order_status WHERE status_key = ? AND order_id = ?";
        List<OrderStatus> results = jdbcTemplate.query(sql, orderStatusRowMapper(), statusKey, orderId);
        return results.isEmpty() ? null : results.get(0);
    }

    public long count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM tbl_order_status", Long.class);
    }

    public int countByOrderId(String orderId) {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM tbl_order_status WHERE order_id = ?", Integer.class, orderId);
    }

    // === COPY FROM Implementation ===

    private int bulkInsertWithCopy(List<OrderStatus> orderStatuses) throws Exception {
        log.info("Bulk inserting {} order status records using COPY FROM", orderStatuses.size());

        String csvData = generateCsvData(orderStatuses);
        return jdbcTemplate.execute((Connection conn) -> {
            CopyManager copyManager = new CopyManager((BaseConnection) conn.unwrap(BaseConnection.class));
            try (StringReader reader = new StringReader(csvData)) {
                return (int) copyManager.copyIn(COPY_SQL, reader);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private int bulkUpsertWithPreDelete(List<OrderStatus> orderStatuses) throws Exception {
        Set<String> compositeKeys = orderStatuses.stream()
                .map(s -> s.getStatusKey() + "|||" + s.getOrderId())
                .collect(Collectors.toSet());
        deleteByCompositeKeys(compositeKeys);
        return bulkInsertWithCopy(orderStatuses);
    }

    private int deleteByCompositeKeys(Set<String> compositeKeys) {
        if (compositeKeys.isEmpty()) return 0;

        if (compositeKeys.size() <= 500) {
            return deleteCompositeKeysBatch(new ArrayList<>(compositeKeys));
        }

        List<String> keyList = new ArrayList<>(compositeKeys);
        int totalDeleted = 0;
        for (int i = 0; i < keyList.size(); i += 500) {
            List<String> batch = keyList.subList(i, Math.min(i + 500, keyList.size()));
            totalDeleted += deleteCompositeKeysBatch(batch);
        }
        return totalDeleted;
    }

    private int deleteCompositeKeysBatch(List<String> compositeKeys) {
        StringBuilder sql = new StringBuilder("DELETE FROM tbl_order_status WHERE ");
        List<Object> params = new ArrayList<>();

        for (int i = 0; i < compositeKeys.size(); i++) {
            String[] parts = compositeKeys.get(i).split("\\|\\|\\|", 2);
            if (parts.length == 2) {
                if (i > 0) sql.append(" OR ");
                sql.append("(status_key = ? AND order_id = ?)");
                params.add(Long.valueOf(parts[0]));
                params.add(parts[1]);
            }
        }
        return jdbcTemplate.update(sql.toString(), params.toArray());
    }

    // Update existing deleteByOrderIds method
    public int deleteByOrderIds(Set<String> orderIds) {
        if (orderIds.isEmpty()) return 0;

        if (orderIds.size() <= 1000) {
            String placeholders = String.join(",", Collections.nCopies(orderIds.size(), "?"));
            String sql = "DELETE FROM tbl_order_status WHERE order_id IN (" + placeholders + ")";
            return jdbcTemplate.update(sql, orderIds.toArray());
        }

        List<String> idList = new ArrayList<>(orderIds);
        int totalDeleted = 0;
        for (int i = 0; i < idList.size(); i += 1000) {
            List<String> batch = idList.subList(i, Math.min(i + 1000, idList.size()));
            String placeholders = String.join(",", Collections.nCopies(batch.size(), "?"));
            String sql = "DELETE FROM tbl_order_status WHERE order_id IN (" + placeholders + ")";
            totalDeleted += jdbcTemplate.update(sql, batch.toArray());
        }
        return totalDeleted;
    }

    private String generateCsvData(List<OrderStatus> orderStatuses) {
        return orderStatuses.stream()
                .map(status -> CsvFormatter.joinCsvRow(
                        status.getStatusKey(), status.getOrderId(), status.getTransitionDateKey(),
                        CsvFormatter.formatDateTime(status.getTransitionTimestamp()),
                        status.getDurationInPreviousStatusHours(), status.getTransitionReason(),
                        status.getTransitionTrigger(), status.getChangedBy(),
                        CsvFormatter.formatBoolean(status.getIsOnTimeTransition()),
                        CsvFormatter.formatBoolean(status.getIsExpectedTransition()), status.getHistoryKey()
                ))
                .collect(java.util.stream.Collectors.joining("\n"));
    }

    private int executeBatchUpsert(List<OrderStatus> orderStatuses) {
        log.info("Batch upserting {} order status records", orderStatuses.size());
        return jdbcTemplate.batchUpdate(UPSERT_SQL, orderStatuses.stream()
                .map(this::mapToParams).toList()).length;
    }

    private Object[] mapToParams(OrderStatus s) {
        return new Object[]{
                s.getStatusKey(), s.getOrderId(), s.getTransitionDateKey(), s.getTransitionTimestamp(),
                s.getDurationInPreviousStatusHours(), s.getTransitionReason(), s.getTransitionTrigger(),
                s.getChangedBy(), s.getIsOnTimeTransition(), s.getIsExpectedTransition(), s.getHistoryKey()
        };
    }

    private RowMapper<OrderStatus> orderStatusRowMapper() {
        return (rs, rowNum) -> OrderStatus.builder()
                .statusKey(rs.getLong("status_key"))
                .orderId(rs.getString("order_id"))
                .transitionDateKey(rs.getInt("transition_date_key"))
                .transitionTimestamp(rs.getTimestamp("transition_timestamp") != null ?
                        rs.getTimestamp("transition_timestamp").toLocalDateTime() : null)
                .durationInPreviousStatusHours(rs.getInt("duration_in_previous_status_hours"))
                .transitionReason(rs.getString("transition_reason"))
                .transitionTrigger(rs.getString("transition_trigger"))
                .changedBy(rs.getString("changed_by"))
                .isOnTimeTransition(rs.getBoolean("is_on_time_transition"))
                .isExpectedTransition(rs.getBoolean("is_expected_transition"))
                .historyKey(rs.getLong("history_key"))
                .build();
    }
}