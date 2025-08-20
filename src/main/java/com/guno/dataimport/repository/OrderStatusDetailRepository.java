package com.guno.dataimport.repository;

import com.guno.dataimport.entity.OrderStatusDetail;
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
 * OrderStatusDetail Repository - JDBC operations with COPY FROM optimization
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class OrderStatusDetailRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final String UPSERT_SQL = """
        INSERT INTO tbl_order_status_detail (
            status_key, order_id, is_active_order, is_completed_order, is_revenue_recognized,
            is_refundable, is_cancellable, is_trackable, next_possible_statuses,
            auto_transition_hours, requires_manual_action, status_color, status_icon,
            customer_visible, customer_description, average_duration_hours, success_rate
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (status_key, order_id) DO UPDATE SET
            is_active_order = EXCLUDED.is_active_order,
            is_completed_order = EXCLUDED.is_completed_order,
            is_revenue_recognized = EXCLUDED.is_revenue_recognized,
            is_refundable = EXCLUDED.is_refundable,
            is_cancellable = EXCLUDED.is_cancellable,
            is_trackable = EXCLUDED.is_trackable,
            next_possible_statuses = EXCLUDED.next_possible_statuses,
            requires_manual_action = EXCLUDED.requires_manual_action,
            customer_visible = EXCLUDED.customer_visible,
            customer_description = EXCLUDED.customer_description
        """;

    private static final String COPY_SQL = """
        COPY tbl_order_status_detail (
            status_key, order_id, is_active_order, is_completed_order, is_revenue_recognized,
            is_refundable, is_cancellable, is_trackable, next_possible_statuses,
            auto_transition_hours, requires_manual_action, status_color, status_icon,
            customer_visible, customer_description, average_duration_hours, success_rate
        ) FROM STDIN WITH (FORMAT CSV, DELIMITER ',')
        """;

    /**
     * OPTIMIZED: Bulk upsert with COPY FROM fallback
     */
    public int bulkUpsert(List<OrderStatusDetail> orderStatusDetails) {
        if (orderStatusDetails == null || orderStatusDetails.isEmpty()) return 0;

        try {
            return bulkUpsertWithPreDelete(orderStatusDetails);
        } catch (Exception e) {
            log.warn("COPY FROM failed, using batch upsert: {}", e.getMessage());
            return executeBatchUpsert(orderStatusDetails);
        }
    }

    public List<OrderStatusDetail> findByOrderIds(Set<String> orderIds) {
        if (orderIds == null || orderIds.isEmpty()) return List.of();

        String sql = "SELECT * FROM tbl_order_status_detail WHERE order_id = ANY(?)";
        return jdbcTemplate.query(sql, orderStatusDetailRowMapper(), orderIds.toArray(new String[0]));
    }

    public List<OrderStatusDetail> findByOrderId(String orderId) {
        String sql = "SELECT * FROM tbl_order_status_detail WHERE order_id = ?";
        return jdbcTemplate.query(sql, orderStatusDetailRowMapper(), orderId);
    }

    public OrderStatusDetail findByStatusKeyAndOrderId(Long statusKey, String orderId) {
        String sql = "SELECT * FROM tbl_order_status_detail WHERE status_key = ? AND order_id = ?";
        List<OrderStatusDetail> results = jdbcTemplate.query(sql, orderStatusDetailRowMapper(), statusKey, orderId);
        return results.isEmpty() ? null : results.get(0);
    }

    public OrderStatusDetail findActiveByOrderId(String orderId) {
        String sql = """
            SELECT osd.* FROM tbl_order_status_detail osd
            JOIN tbl_order_status os ON osd.status_key = os.status_key AND osd.order_id = os.order_id
            WHERE osd.order_id = ? AND osd.is_active_order = true
            ORDER BY os.transition_timestamp DESC
            LIMIT 1
            """;
        List<OrderStatusDetail> results = jdbcTemplate.query(sql, orderStatusDetailRowMapper(), orderId);
        return results.isEmpty() ? null : results.get(0);
    }

    public long count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM tbl_order_status_detail", Long.class);
    }

    public int countByOrderId(String orderId) {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM tbl_order_status_detail WHERE order_id = ?", Integer.class, orderId);
    }

    // === COPY FROM Implementation ===

    private int bulkInsertWithCopy(List<OrderStatusDetail> orderStatusDetails) throws Exception {
        log.info("Bulk inserting {} order status detail records using COPY FROM", orderStatusDetails.size());

        String csvData = generateCsvData(orderStatusDetails);
        return jdbcTemplate.execute((Connection conn) -> {
            CopyManager copyManager = new CopyManager((BaseConnection) conn.unwrap(BaseConnection.class));
            try (StringReader reader = new StringReader(csvData)) {
                return (int) copyManager.copyIn(COPY_SQL, reader);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private int bulkUpsertWithPreDelete(List<OrderStatusDetail> orderStatusDetails) throws Exception {
        Set<String> compositeKeys = orderStatusDetails.stream()
                .map(d -> d.getStatusKey() + "|||" + d.getOrderId())
                .collect(Collectors.toSet());
        deleteByCompositeKeys(compositeKeys);
        return bulkInsertWithCopy(orderStatusDetails);
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
        StringBuilder sql = new StringBuilder("DELETE FROM tbl_order_status_detail WHERE ");
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
            String sql = "DELETE FROM tbl_order_status_detail WHERE order_id IN (" + placeholders + ")";
            return jdbcTemplate.update(sql, orderIds.toArray());
        }

        List<String> idList = new ArrayList<>(orderIds);
        int totalDeleted = 0;
        for (int i = 0; i < idList.size(); i += 1000) {
            List<String> batch = idList.subList(i, Math.min(i + 1000, idList.size()));
            String placeholders = String.join(",", Collections.nCopies(batch.size(), "?"));
            String sql = "DELETE FROM tbl_order_status_detail WHERE order_id IN (" + placeholders + ")";
            totalDeleted += jdbcTemplate.update(sql, batch.toArray());
        }
        return totalDeleted;
    }

    private String generateCsvData(List<OrderStatusDetail> orderStatusDetails) {
        return orderStatusDetails.stream()
                .map(detail -> CsvFormatter.joinCsvRow(
                        detail.getStatusKey(), detail.getOrderId(),
                        CsvFormatter.formatBoolean(detail.getIsActiveOrder()),
                        CsvFormatter.formatBoolean(detail.getIsCompletedOrder()),
                        CsvFormatter.formatBoolean(detail.getIsRevenueRecognized()),
                        CsvFormatter.formatBoolean(detail.getIsRefundable()),
                        CsvFormatter.formatBoolean(detail.getIsCancellable()),
                        CsvFormatter.formatBoolean(detail.getIsTrackable()),
                        detail.getNextPossibleStatuses(), detail.getAutoTransitionHours(),
                        CsvFormatter.formatBoolean(detail.getRequiresManualAction()),
                        detail.getStatusColor(), detail.getStatusIcon(),
                        CsvFormatter.formatBoolean(detail.getCustomerVisible()),
                        detail.getCustomerDescription(), detail.getAverageDurationHours(), detail.getSuccessRate()
                ))
                .collect(java.util.stream.Collectors.joining("\n"));
    }

    private int executeBatchUpsert(List<OrderStatusDetail> orderStatusDetails) {
        log.info("Batch upserting {} order status detail records", orderStatusDetails.size());
        return jdbcTemplate.batchUpdate(UPSERT_SQL, orderStatusDetails.stream()
                .map(this::mapToParams).toList()).length;
    }

    private Object[] mapToParams(OrderStatusDetail d) {
        return new Object[]{
                d.getStatusKey(), d.getOrderId(), d.getIsActiveOrder(), d.getIsCompletedOrder(),
                d.getIsRevenueRecognized(), d.getIsRefundable(), d.getIsCancellable(), d.getIsTrackable(),
                d.getNextPossibleStatuses(), d.getAutoTransitionHours(), d.getRequiresManualAction(),
                d.getStatusColor(), d.getStatusIcon(), d.getCustomerVisible(), d.getCustomerDescription(),
                d.getAverageDurationHours(), d.getSuccessRate()
        };
    }

    private RowMapper<OrderStatusDetail> orderStatusDetailRowMapper() {
        return (rs, rowNum) -> OrderStatusDetail.builder()
                .statusKey(rs.getLong("status_key"))
                .orderId(rs.getString("order_id"))
                .isActiveOrder(rs.getBoolean("is_active_order"))
                .isCompletedOrder(rs.getBoolean("is_completed_order"))
                .isRevenueRecognized(rs.getBoolean("is_revenue_recognized"))
                .isRefundable(rs.getBoolean("is_refundable"))
                .isCancellable(rs.getBoolean("is_cancellable"))
                .isTrackable(rs.getBoolean("is_trackable"))
                .nextPossibleStatuses(rs.getString("next_possible_statuses"))
                .autoTransitionHours(rs.getInt("auto_transition_hours"))
                .requiresManualAction(rs.getBoolean("requires_manual_action"))
                .statusColor(rs.getString("status_color"))
                .statusIcon(rs.getString("status_icon"))
                .customerVisible(rs.getBoolean("customer_visible"))
                .customerDescription(rs.getString("customer_description"))
                .averageDurationHours(rs.getDouble("average_duration_hours"))
                .successRate(rs.getDouble("success_rate"))
                .build();
    }
}