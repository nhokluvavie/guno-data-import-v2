package com.guno.dataimport.repository;

import com.guno.dataimport.entity.OrderItem;
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
 * OrderItem Repository - JDBC operations with optimized COPY FROM
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

    private static final String COPY_SQL = """
        COPY tbl_order_item (
            order_id, sku, platform_product_id, quantity, unit_price, total_price,
            item_discount, promotion_type, promotion_code, item_status,
            item_sequence, op_id
        ) FROM STDIN WITH (FORMAT CSV, DELIMITER ',')
        """;

    /**
     * OPTIMIZED: Bulk insert with COPY FROM
     */
    public int bulkUpsert(List<OrderItem> orderItems) {
        if (orderItems == null || orderItems.isEmpty()) return 0;
        try {
            return tempTableUpsert(orderItems, "tbl_order_item",
                    "order_id, sku, platform_product_id", "quantity = EXCLUDED.quantity, total_price = EXCLUDED.total_price");
        } catch (Exception e) {
            log.warn("Temp table failed, using batch: {}", e.getMessage());
            return executeBatchInsert(orderItems);
        }
    }

    public List<OrderItem> findByOrderId(String orderId) {
        String sql = "SELECT * FROM tbl_order_item WHERE order_id = ? ORDER BY item_sequence";
        return jdbcTemplate.query(sql, orderItemRowMapper(), orderId);
    }

    public List<OrderItem> findByOrderIds(Set<String> orderIds) {
        if (orderIds == null || orderIds.isEmpty()) return List.of();

        String sql = "SELECT * FROM tbl_order_item WHERE order_id = ANY(?) ORDER BY order_id, item_sequence";
        return jdbcTemplate.query(sql, orderItemRowMapper(), orderIds.toArray(new String[0]));
    }

    public long count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM tbl_order_item", Long.class);
    }

    public int countByOrderId(String orderId) {
        return jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM tbl_order_item WHERE order_id = ?", Integer.class, orderId);
    }

    // === COPY FROM Implementation ===

    public int bulkInsertWithCopy(List<OrderItem> orderItems) throws Exception {
        log.info("Bulk inserting {} order items using COPY FROM", orderItems.size());

        String csvData = generateCsvData(orderItems);
        return jdbcTemplate.execute((Connection conn) -> {
            CopyManager copyManager = new CopyManager((BaseConnection) conn.unwrap(BaseConnection.class));
            try (StringReader reader = new StringReader(csvData)) {
                return (int) copyManager.copyIn(COPY_SQL, reader);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    // Update existing deleteByOrderIds method
    public int deleteByOrderIds(Set<String> orderIds) {
        if (orderIds.isEmpty()) return 0;

        if (orderIds.size() <= 1000) {
            String placeholders = String.join(",", Collections.nCopies(orderIds.size(), "?"));
            String sql = "DELETE FROM tbl_order_item WHERE order_id IN (" + placeholders + ")";
            return jdbcTemplate.update(sql, orderIds.toArray());
        }

        List<String> idList = new ArrayList<>(orderIds);
        int totalDeleted = 0;
        for (int i = 0; i < idList.size(); i += 1000) {
            List<String> batch = idList.subList(i, Math.min(i + 1000, idList.size()));
            String placeholders = String.join(",", Collections.nCopies(batch.size(), "?"));
            String sql = "DELETE FROM tbl_order_item WHERE order_id IN (" + placeholders + ")";
            totalDeleted += jdbcTemplate.update(sql, batch.toArray());
        }
        return totalDeleted;
    }

    private <T> int tempTableUpsert(List<OrderItem> entities, String tableName, String conflictColumns, String updateSet) throws Exception {
        String tempTable = "temp_" + tableName.substring(4) + "_" + System.currentTimeMillis();

        try {
            // Create temp table
            jdbcTemplate.execute("CREATE TEMP TABLE " + tempTable + " (LIKE " + tableName + " INCLUDING DEFAULTS)");

            // COPY INTO temp table
            String tempCopySQL = COPY_SQL.replace(tableName, tempTable);
            String csvData = generateCsvData(entities);

            jdbcTemplate.execute((Connection conn) -> {
                CopyManager copyManager = new CopyManager((BaseConnection) conn.unwrap(BaseConnection.class));
                try (StringReader reader = new StringReader(csvData)) {
                    return (int) copyManager.copyIn(tempCopySQL, reader);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            // MERGE to main table
            String mergeSQL = String.format(
                    "INSERT INTO %s SELECT * FROM %s ON CONFLICT (%s) DO UPDATE SET %s",
                    tableName, tempTable, conflictColumns, updateSet);

            return jdbcTemplate.update(mergeSQL);

        } finally {
            jdbcTemplate.execute("DROP TABLE IF EXISTS " + tempTable);
        }
    }

    private String generateCsvData(List<OrderItem> orderItems) {
        return orderItems.stream()
                .map(item -> CsvFormatter.joinCsvRow(
                        item.getOrderId(), item.getSku(), item.getPlatformProductId(),
                        item.getQuantity(), item.getUnitPrice(), item.getTotalPrice(),
                        item.getItemDiscount(), item.getPromotionType(), item.getPromotionCode(),
                        item.getItemStatus(), item.getItemSequence(), item.getOpId()
                ))
                .collect(java.util.stream.Collectors.joining("\n"));
    }

    private int executeBatchInsert(List<OrderItem> orderItems) {
        if (orderItems == null || orderItems.isEmpty()) {
            return 0;
        }

        final int CHUNK_SIZE = 1000;
        int totalProcessed = 0;

        log.info("📦 Batch inserting {} order items in chunks of {}",
                orderItems.size(), CHUNK_SIZE);

        for (int i = 0; i < orderItems.size(); i += CHUNK_SIZE) {
            int end = Math.min(i + CHUNK_SIZE, orderItems.size());
            List<OrderItem> chunk = orderItems.subList(i, end);

            int[] counts = jdbcTemplate.batchUpdate(
                    INSERT_SQL,
                    chunk.stream().map(this::mapToParams).toList()
            );
            totalProcessed += counts.length;
        }

        log.info("✅ Order item batch completed: {} records", totalProcessed);
        return totalProcessed;
    }

    private Object[] mapToParams(OrderItem item) {
        return new Object[]{
                item.getOrderId(), item.getSku(), item.getPlatformProductId(),
                item.getQuantity(), item.getUnitPrice(), item.getTotalPrice(),
                item.getItemDiscount(), item.getPromotionType(), item.getPromotionCode(),
                item.getItemStatus(), item.getItemSequence(), item.getOpId()
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