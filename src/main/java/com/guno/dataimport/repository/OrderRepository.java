package com.guno.dataimport.repository;

import com.guno.dataimport.entity.Order;
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
import java.util.*;
import java.util.stream.Collectors;

/**
 * Order Repository - JDBC operations with COPY FROM optimization
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class OrderRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final String UPSERT_SQL = """
        INSERT INTO tbl_order (
            order_id, customer_id, shop_id, internal_uuid, order_count, item_quantity,
            total_items_in_order, gross_revenue, net_revenue, shipping_fee, tax_amount,
            discount_amount, cod_amount, platform_fee, seller_discount, platform_discount,
            original_price, estimated_shipping_fee, actual_shipping_fee, shipping_weight_gram,
            days_to_ship, is_delivered, is_cancelled, is_returned, is_cod, is_new_customer,
            is_repeat_customer, is_bulk_order, is_promotional_order, is_same_day_delivery,
            order_to_ship_hours, ship_to_delivery_hours, total_fulfillment_hours,
            customer_order_sequence, customer_lifetime_orders, customer_lifetime_value,
            days_since_last_order, promotion_impact, ad_revenue, organic_revenue,
            aov, shipping_cost_ratio, created_at, raw_data, platform_specific_data
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (order_id) DO UPDATE SET
            shop_id = EXCLUDED.shop_id,
            item_quantity = EXCLUDED.item_quantity,
            gross_revenue = EXCLUDED.gross_revenue,
            net_revenue = EXCLUDED.net_revenue,
            shipping_fee = EXCLUDED.shipping_fee,
            is_delivered = EXCLUDED.is_delivered,
            is_cancelled = EXCLUDED.is_cancelled,
            is_returned = EXCLUDED.is_returned
        """;

    private static final String COPY_SQL = """
        COPY tbl_order (
            order_id, customer_id, shop_id, internal_uuid, order_count, item_quantity,
            total_items_in_order, gross_revenue, net_revenue, shipping_fee, tax_amount,
            discount_amount, cod_amount, platform_fee, seller_discount, platform_discount,
            original_price, estimated_shipping_fee, actual_shipping_fee, shipping_weight_gram,
            days_to_ship, is_delivered, is_cancelled, is_returned, is_cod, is_new_customer,
            is_repeat_customer, is_bulk_order, is_promotional_order, is_same_day_delivery,
            order_to_ship_hours, ship_to_delivery_hours, total_fulfillment_hours,
            customer_order_sequence, customer_lifetime_orders, customer_lifetime_value,
            days_since_last_order, promotion_impact, ad_revenue, organic_revenue,
            aov, shipping_cost_ratio, created_at, raw_data, platform_specific_data
        ) FROM STDIN WITH (FORMAT CSV, DELIMITER ',')
        """;

    /**
     * OPTIMIZED: Bulk upsert with COPY FROM fallback
     */
    public int bulkUpsert(List<Order> orders) {
        if (orders == null || orders.isEmpty()) return 0;

        try {
            return bulkUpsertWithPreDelete(orders);
        } catch (Exception e) {
            log.warn("COPY FROM failed, using batch upsert: {}", e.getMessage());
            return executeBatchUpsert(orders);
        }
    }

    /**
     * Find existing orders by IDs
     */
    public Map<String, Order> findByIds(Set<String> orderIds) {
        if (orderIds == null || orderIds.isEmpty()) return Map.of();

        String sql = "SELECT * FROM tbl_order WHERE order_id = ANY(?)";
        return jdbcTemplate.query(sql, orderRowMapper(), orderIds.toArray(new String[0]))
                .stream().collect(java.util.stream.Collectors.toMap(
                        Order::getOrderId, order -> order));
    }

    public boolean exists(String orderId) {
        return !jdbcTemplate.queryForList("SELECT 1 FROM tbl_order WHERE order_id = ?", orderId).isEmpty();
    }

    public List<Order> findByCustomerId(String customerId) {
        String sql = "SELECT * FROM tbl_order WHERE customer_id = ? ORDER BY created_at DESC";
        return jdbcTemplate.query(sql, orderRowMapper(), customerId);
    }

    public long count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM tbl_order", Long.class);
    }

    // === COPY FROM Implementation ===

    private int bulkInsertWithCopy(List<Order> orders) throws Exception {
        log.info("Bulk inserting {} orders using COPY FROM", orders.size());

        String csvData = generateCsvData(orders);
        return jdbcTemplate.execute((Connection conn) -> {
            CopyManager copyManager = new CopyManager((BaseConnection) conn.unwrap(BaseConnection.class));
            try (StringReader reader = new StringReader(csvData)) {
                return (int) copyManager.copyIn(COPY_SQL, reader);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private int bulkUpsertWithPreDelete(List<Order> orders) throws Exception {
        Set<String> orderIds = orders.stream()
                .map(Order::getOrderId).collect(Collectors.toSet());
        deleteByIds(orderIds);
        return bulkInsertWithCopy(orders);
    }

    private int deleteByIds(Set<String> orderIds) {
        if (orderIds.isEmpty()) return 0;

        if (orderIds.size() <= 1000) {
            String placeholders = String.join(",", Collections.nCopies(orderIds.size(), "?"));
            String sql = "DELETE FROM tbl_order WHERE order_id IN (" + placeholders + ")";
            return jdbcTemplate.update(sql, orderIds.toArray());
        }

        List<String> idList = new ArrayList<>(orderIds);
        int totalDeleted = 0;
        for (int i = 0; i < idList.size(); i += 1000) {
            List<String> batch = idList.subList(i, Math.min(i + 1000, idList.size()));
            String placeholders = String.join(",", Collections.nCopies(batch.size(), "?"));
            String sql = "DELETE FROM tbl_order WHERE order_id IN (" + placeholders + ")";
            totalDeleted += jdbcTemplate.update(sql, batch.toArray());
        }
        return totalDeleted;
    }

    private String generateCsvData(List<Order> orders) {
        return orders.stream()
                .map(order -> CsvFormatter.joinCsvRow(
                        order.getOrderId(), order.getCustomerId(), order.getShopId(), order.getInternalUuid(),
                        order.getOrderCount(), order.getItemQuantity(), order.getTotalItemsInOrder(),
                        order.getGrossRevenue(), order.getNetRevenue(), order.getShippingFee(), order.getTaxAmount(),
                        order.getDiscountAmount(), order.getCodAmount(), order.getPlatformFee(), order.getSellerDiscount(),
                        order.getPlatformDiscount(), order.getOriginalPrice(), order.getEstimatedShippingFee(),
                        order.getActualShippingFee(), order.getShippingWeightGram(), order.getDaysToShip(),
                        CsvFormatter.formatBoolean(order.getIsDelivered()), CsvFormatter.formatBoolean(order.getIsCancelled()),
                        CsvFormatter.formatBoolean(order.getIsReturned()), CsvFormatter.formatBoolean(order.getIsCod()),
                        CsvFormatter.formatBoolean(order.getIsNewCustomer()), CsvFormatter.formatBoolean(order.getIsRepeatCustomer()),
                        CsvFormatter.formatBoolean(order.getIsBulkOrder()), CsvFormatter.formatBoolean(order.getIsPromotionalOrder()),
                        CsvFormatter.formatBoolean(order.getIsSameDayDelivery()), order.getOrderToShipHours(),
                        order.getShipToDeliveryHours(), order.getTotalFulfillmentHours(), order.getCustomerOrderSequence(),
                        order.getCustomerLifetimeOrders(), order.getCustomerLifetimeValue(), order.getDaysSinceLastOrder(),
                        order.getPromotionImpact(), order.getAdRevenue(), order.getOrganicRevenue(), order.getAov(),
                        order.getShippingCostRatio(), CsvFormatter.formatDateTime(order.getCreatedAt()),
                        order.getRawData(), order.getPlatformSpecificData()
                ))
                .collect(java.util.stream.Collectors.joining("\n"));
    }

    private int executeBatchUpsert(List<Order> orders) {
        log.info("Batch upserting {} orders", orders.size());
        return jdbcTemplate.batchUpdate(UPSERT_SQL, orders.stream()
                .map(this::mapToParams).toList()).length;
    }

    private Object[] mapToParams(Order o) {
        return new Object[]{
                o.getOrderId(), o.getCustomerId(), o.getShopId(), o.getInternalUuid(), o.getOrderCount(),
                o.getItemQuantity(), o.getTotalItemsInOrder(), o.getGrossRevenue(), o.getNetRevenue(),
                o.getShippingFee(), o.getTaxAmount(), o.getDiscountAmount(), o.getCodAmount(), o.getPlatformFee(),
                o.getSellerDiscount(), o.getPlatformDiscount(), o.getOriginalPrice(), o.getEstimatedShippingFee(),
                o.getActualShippingFee(), o.getShippingWeightGram(), o.getDaysToShip(), o.getIsDelivered(),
                o.getIsCancelled(), o.getIsReturned(), o.getIsCod(), o.getIsNewCustomer(), o.getIsRepeatCustomer(),
                o.getIsBulkOrder(), o.getIsPromotionalOrder(), o.getIsSameDayDelivery(), o.getOrderToShipHours(),
                o.getShipToDeliveryHours(), o.getTotalFulfillmentHours(), o.getCustomerOrderSequence(),
                o.getCustomerLifetimeOrders(), o.getCustomerLifetimeValue(), o.getDaysSinceLastOrder(),
                o.getPromotionImpact(), o.getAdRevenue(), o.getOrganicRevenue(), o.getAov(), o.getShippingCostRatio(),
                o.getCreatedAt(), o.getRawData(), o.getPlatformSpecificData()
        };
    }

    private RowMapper<Order> orderRowMapper() {
        return (rs, rowNum) -> Order.builder()
                .orderId(rs.getString("order_id"))
                .customerId(rs.getString("customer_id"))
                .shopId(rs.getString("shop_id"))
                .internalUuid(rs.getString("internal_uuid"))
                .orderCount(rs.getInt("order_count"))
                .itemQuantity(rs.getInt("item_quantity"))
                .totalItemsInOrder(rs.getInt("total_items_in_order"))
                .grossRevenue(rs.getDouble("gross_revenue"))
                .netRevenue(rs.getDouble("net_revenue"))
                .shippingFee(rs.getDouble("shipping_fee"))
                .taxAmount(rs.getDouble("tax_amount"))
                .discountAmount(rs.getDouble("discount_amount"))
                .codAmount(rs.getDouble("cod_amount"))
                .platformFee(rs.getDouble("platform_fee"))
                .sellerDiscount(rs.getDouble("seller_discount"))
                .platformDiscount(rs.getDouble("platform_discount"))
                .originalPrice(rs.getDouble("original_price"))
                .estimatedShippingFee(rs.getDouble("estimated_shipping_fee"))
                .actualShippingFee(rs.getDouble("actual_shipping_fee"))
                .shippingWeightGram(rs.getInt("shipping_weight_gram"))
                .daysToShip(rs.getInt("days_to_ship"))
                .isDelivered(rs.getBoolean("is_delivered"))
                .isCancelled(rs.getBoolean("is_cancelled"))
                .isReturned(rs.getBoolean("is_returned"))
                .isCod(rs.getBoolean("is_cod"))
                .isNewCustomer(rs.getBoolean("is_new_customer"))
                .isRepeatCustomer(rs.getBoolean("is_repeat_customer"))
                .isBulkOrder(rs.getBoolean("is_bulk_order"))
                .isPromotionalOrder(rs.getBoolean("is_promotional_order"))
                .isSameDayDelivery(rs.getBoolean("is_same_day_delivery"))
                .orderToShipHours(rs.getInt("order_to_ship_hours"))
                .shipToDeliveryHours(rs.getInt("ship_to_delivery_hours"))
                .totalFulfillmentHours(rs.getInt("total_fulfillment_hours"))
                .customerOrderSequence(rs.getInt("customer_order_sequence"))
                .customerLifetimeOrders(rs.getInt("customer_lifetime_orders"))
                .customerLifetimeValue(rs.getDouble("customer_lifetime_value"))
                .daysSinceLastOrder(rs.getInt("days_since_last_order"))
                .promotionImpact(rs.getDouble("promotion_impact"))
                .adRevenue(rs.getDouble("ad_revenue"))
                .organicRevenue(rs.getDouble("organic_revenue"))
                .aov(rs.getDouble("aov"))
                .shippingCostRatio(rs.getDouble("shipping_cost_ratio"))
                .createdAt(rs.getTimestamp("created_at") != null ?
                        rs.getTimestamp("created_at").toLocalDateTime() : null)
                .rawData(rs.getInt("raw_data"))
                .platformSpecificData(rs.getInt("platform_specific_data"))
                .build();
    }
}