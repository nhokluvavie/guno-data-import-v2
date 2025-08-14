package com.guno.dataimport.repository;

import com.guno.dataimport.entity.OrderStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.Set;

/**
 * OrderStatus Repository - JDBC operations for OrderStatus entity
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

    // Bulk upsert order status records
    public int bulkUpsert(List<OrderStatus> orderStatuses) {
        if (orderStatuses == null || orderStatuses.isEmpty()) {
            return 0;
        }

        log.info("Bulk upserting {} order status records", orderStatuses.size());

        return jdbcTemplate.batchUpdate(UPSERT_SQL, orderStatuses.stream()
                .map(this::mapOrderStatusToParams)
                .toList()
        ).length;
    }

    // Find order statuses by order IDs
    public List<OrderStatus> findByOrderIds(Set<String> orderIds) {
        if (orderIds == null || orderIds.isEmpty()) {
            return List.of();
        }

        String sql = "SELECT * FROM tbl_order_status WHERE order_id = ANY(?) ORDER BY transition_timestamp DESC";
        return jdbcTemplate.query(sql, orderStatusRowMapper(),
                orderIds.toArray(new String[0]));
    }

    // Find by order ID
    public List<OrderStatus> findByOrderId(String orderId) {
        String sql = "SELECT * FROM tbl_order_status WHERE order_id = ? ORDER BY transition_timestamp DESC";
        return jdbcTemplate.query(sql, orderStatusRowMapper(), orderId);
    }

    // Find current status for order
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

    // Find by status key and order ID (composite key)
    public OrderStatus findByStatusKeyAndOrderId(Long statusKey, String orderId) {
        String sql = "SELECT * FROM tbl_order_status WHERE status_key = ? AND order_id = ?";
        List<OrderStatus> results = jdbcTemplate.query(sql, orderStatusRowMapper(), statusKey, orderId);
        return results.isEmpty() ? null : results.get(0);
    }

    // Delete by order IDs
    public int deleteByOrderIds(Set<String> orderIds) {
        if (orderIds == null || orderIds.isEmpty()) {
            return 0;
        }

        String sql = "DELETE FROM tbl_order_status WHERE order_id = ANY(?)";
        return jdbcTemplate.update(sql, orderIds.toArray(new String[0]));
    }

    // Get total count
    public long count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM tbl_order_status", Long.class);
    }

    // Get status history count for order
    public int countByOrderId(String orderId) {
        String sql = "SELECT COUNT(*) FROM tbl_order_status WHERE order_id = ?";
        return jdbcTemplate.queryForObject(sql, Integer.class, orderId);
    }

    // Helper methods
    private Object[] mapOrderStatusToParams(OrderStatus orderStatus) {
        return new Object[]{
                orderStatus.getStatusKey(),
                orderStatus.getOrderId(),
                orderStatus.getTransitionDateKey(),
                orderStatus.getTransitionTimestamp(),
                orderStatus.getDurationInPreviousStatusHours(),
                orderStatus.getTransitionReason(),
                orderStatus.getTransitionTrigger(),
                orderStatus.getChangedBy(),
                orderStatus.getIsOnTimeTransition(),
                orderStatus.getIsExpectedTransition(),
                orderStatus.getHistoryKey()
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