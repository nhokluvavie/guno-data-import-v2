package com.guno.dataimport.repository;

import com.guno.dataimport.entity.OrderStatusDetail;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.Set;

/**
 * OrderStatusDetail Repository - JDBC operations for OrderStatusDetail entity
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

    // Bulk upsert order status detail records
    public int bulkUpsert(List<OrderStatusDetail> orderStatusDetails) {
        if (orderStatusDetails == null || orderStatusDetails.isEmpty()) {
            return 0;
        }

        log.info("Bulk upserting {} order status detail records", orderStatusDetails.size());

        return jdbcTemplate.batchUpdate(UPSERT_SQL, orderStatusDetails.stream()
                .map(this::mapOrderStatusDetailToParams)
                .toList()
        ).length;
    }

    // Find order status details by order IDs
    public List<OrderStatusDetail> findByOrderIds(Set<String> orderIds) {
        if (orderIds == null || orderIds.isEmpty()) {
            return List.of();
        }

        String sql = "SELECT * FROM tbl_order_status_detail WHERE order_id = ANY(?)";
        return jdbcTemplate.query(sql, orderStatusDetailRowMapper(),
                orderIds.toArray(new String[0]));
    }

    // Find by order ID
    public List<OrderStatusDetail> findByOrderId(String orderId) {
        String sql = "SELECT * FROM tbl_order_status_detail WHERE order_id = ?";
        return jdbcTemplate.query(sql, orderStatusDetailRowMapper(), orderId);
    }

    // Find by status key and order ID (composite key)
    public OrderStatusDetail findByStatusKeyAndOrderId(Long statusKey, String orderId) {
        String sql = "SELECT * FROM tbl_order_status_detail WHERE status_key = ? AND order_id = ?";
        List<OrderStatusDetail> results = jdbcTemplate.query(sql, orderStatusDetailRowMapper(), statusKey, orderId);
        return results.isEmpty() ? null : results.get(0);
    }

    // Find current active status detail for order
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

    // Delete by order IDs
    public int deleteByOrderIds(Set<String> orderIds) {
        if (orderIds == null || orderIds.isEmpty()) {
            return 0;
        }

        String sql = "DELETE FROM tbl_order_status_detail WHERE order_id = ANY(?)";
        return jdbcTemplate.update(sql, orderIds.toArray(new String[0]));
    }

    // Get total count
    public long count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM tbl_order_status_detail", Long.class);
    }

    // Get detail count for order
    public int countByOrderId(String orderId) {
        String sql = "SELECT COUNT(*) FROM tbl_order_status_detail WHERE order_id = ?";
        return jdbcTemplate.queryForObject(sql, Integer.class, orderId);
    }

    // Helper methods
    private Object[] mapOrderStatusDetailToParams(OrderStatusDetail detail) {
        return new Object[]{
                detail.getStatusKey(),
                detail.getOrderId(),
                detail.getIsActiveOrder(),
                detail.getIsCompletedOrder(),
                detail.getIsRevenueRecognized(),
                detail.getIsRefundable(),
                detail.getIsCancellable(),
                detail.getIsTrackable(),
                detail.getNextPossibleStatuses(),
                detail.getAutoTransitionHours(),
                detail.getRequiresManualAction(),
                detail.getStatusColor(),
                detail.getStatusIcon(),
                detail.getCustomerVisible(),
                detail.getCustomerDescription(),
                detail.getAverageDurationHours(),
                detail.getSuccessRate()
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