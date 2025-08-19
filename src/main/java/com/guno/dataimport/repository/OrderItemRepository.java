package com.guno.dataimport.repository;

import com.guno.dataimport.entity.OrderItem;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import java.sql.Array;
import java.util.List;
import java.util.Set;

/**
 * OrderItem Repository - JDBC operations for OrderItem entity
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class OrderItemRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final String INSERT_SQL = """
        INSERT INTO tbl_order_item (
            order_id, sku, platform_product_id, quantity, unit_price, total_price,
            item_discount, promotion_type, promotion_code, item_status,
            item_sequence, op_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """;

    // Bulk insert order items (delete + insert approach)
    public int bulkInsert(List<OrderItem> orderItems) {
        if (orderItems == null || orderItems.isEmpty()) {
            return 0;
        }

        log.info("Bulk inserting {} order items", orderItems.size());

        return jdbcTemplate.batchUpdate(INSERT_SQL, orderItems.stream()
                .map(this::mapOrderItemToParams)
                .toList()
        ).length;
    }

    // FIXED: Delete order items by order IDs with proper array handling
    public int deleteByOrderIds(Set<String> orderIds) {
        if (orderIds == null || orderIds.isEmpty()) {
            return 0;
        }

        log.info("Deleting order items for {} orders", orderIds.size());

        try {
            // Convert Set to Array for PostgreSQL
            String[] orderIdArray = orderIds.toArray(new String[0]);
            Array sqlArray = jdbcTemplate.getDataSource().getConnection().createArrayOf("varchar", orderIdArray);

            String sql = "DELETE FROM tbl_order_item WHERE order_id = ANY(?)";
            return jdbcTemplate.update(sql, sqlArray);

        } catch (Exception e) {
            log.error("Failed to delete order items: {}", e.getMessage());

            // Fallback: Use IN clause instead of ANY
            if (orderIds.size() <= 1000) { // PostgreSQL IN limit
                return deleteByOrderIdsWithIn(orderIds);
            } else {
                throw new RuntimeException("Too many order IDs for deletion: " + orderIds.size(), e);
            }
        }
    }

    // Fallback method using IN clause
    private int deleteByOrderIdsWithIn(Set<String> orderIds) {
        String placeholders = String.join(",", orderIds.stream().map(id -> "?").toList());
        String sql = "DELETE FROM tbl_order_item WHERE order_id IN (" + placeholders + ")";

        return jdbcTemplate.update(sql, orderIds.toArray());
    }

    // Find order items by order ID
    public List<OrderItem> findByOrderId(String orderId) {
        String sql = "SELECT * FROM tbl_order_item WHERE order_id = ? ORDER BY item_sequence";
        return jdbcTemplate.query(sql, orderItemRowMapper(), orderId);
    }

    // Find order items by multiple order IDs
    public List<OrderItem> findByOrderIds(Set<String> orderIds) {
        if (orderIds == null || orderIds.isEmpty()) {
            return List.of();
        }

        try {
            String[] orderIdArray = orderIds.toArray(new String[0]);
            Array sqlArray = jdbcTemplate.getDataSource().getConnection().createArrayOf("varchar", orderIdArray);

            String sql = "SELECT * FROM tbl_order_item WHERE order_id = ANY(?) ORDER BY order_id, item_sequence";
            return jdbcTemplate.query(sql, orderItemRowMapper(), sqlArray);

        } catch (Exception e) {
            log.warn("Failed to query with ANY, using IN clause: {}", e.getMessage());
            return findByOrderIdsWithIn(orderIds);
        }
    }

    private List<OrderItem> findByOrderIdsWithIn(Set<String> orderIds) {
        String placeholders = String.join(",", orderIds.stream().map(id -> "?").toList());
        String sql = "SELECT * FROM tbl_order_item WHERE order_id IN (" + placeholders + ") ORDER BY order_id, item_sequence";

        return jdbcTemplate.query(sql, orderItemRowMapper(), orderIds.toArray());
    }

    // Get total item count
    public long count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM tbl_order_item", Long.class);
    }

    // Get item count by order ID
    public int countByOrderId(String orderId) {
        String sql = "SELECT COUNT(*) FROM tbl_order_item WHERE order_id = ?";
        return jdbcTemplate.queryForObject(sql, Integer.class, orderId);
    }

    // FIXED: Bulk delete + insert (refresh strategy) with proper error handling
    public int bulkRefresh(Set<String> orderIds, List<OrderItem> newOrderItems) {
        if (orderIds == null || orderIds.isEmpty()) {
            return bulkInsert(newOrderItems);
        }

        log.info("Refreshing order items for {} orders with {} new items",
                orderIds.size(), newOrderItems != null ? newOrderItems.size() : 0);

        try {
            // Delete existing items
            int deletedCount = deleteByOrderIds(orderIds);

            // Insert new items
            int insertedCount = bulkInsert(newOrderItems);

            log.info("Deleted {} items, inserted {} items", deletedCount, insertedCount);
            return insertedCount;

        } catch (Exception e) {
            log.error("Bulk refresh failed: {}", e.getMessage());
            throw new RuntimeException("Failed to refresh order items", e);
        }
    }

    // Helper methods
    private Object[] mapOrderItemToParams(OrderItem item) {
        return new Object[]{
                item.getOrderId(),
                item.getSku(),
                item.getPlatformProductId(),
                item.getQuantity(),
                item.getUnitPrice(),
                item.getTotalPrice(),
                item.getItemDiscount(),
                item.getPromotionType(),
                item.getPromotionCode(),
                item.getItemStatus(),
                item.getItemSequence(),
                item.getOpId()
        };
    }

    private RowMapper<OrderItem> orderItemRowMapper() {
        return (rs, rowNum) -> OrderItem.builder()
                .orderId(rs.getString("order_id"))
                .sku(rs.getString("sku"))
                .platformProductId(rs.getString("platform_product_id"))
                .quantity(rs.getInt("quantity"))
                .unitPrice(rs.getDouble("unit_price"))
                .totalPrice(rs.getDouble("total_price"))
                .itemDiscount(rs.getDouble("item_discount"))
                .promotionType(rs.getString("promotion_type"))
                .promotionCode(rs.getString("promotion_code"))
                .itemStatus(rs.getString("item_status"))
                .itemSequence(rs.getInt("item_sequence"))
                .opId(rs.getLong("op_id"))
                .build();
    }
}