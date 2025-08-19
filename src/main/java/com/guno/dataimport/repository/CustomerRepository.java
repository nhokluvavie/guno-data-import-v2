package com.guno.dataimport.repository;

import com.guno.dataimport.entity.Customer;
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
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Customer Repository - JDBC operations with COPY FROM optimization
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class CustomerRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final String UPSERT_SQL = """
        INSERT INTO tbl_customer (
            customer_id, customer_key, platform_customer_id, phone_hash, email_hash,
            gender, age_group, customer_segment, customer_tier, acquisition_channel,
            first_order_date, last_order_date, total_orders, total_spent, average_order_value,
            total_items_purchased, days_since_first_order, days_since_last_order,
            purchase_frequency_days, return_rate, cancellation_rate, cod_preference_rate,
            favorite_category, favorite_brand, preferred_payment_method, preferred_platform,
            primary_shipping_province, ships_to_multiple_provinces, loyalty_points,
            referral_count, is_referrer
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (customer_id) DO UPDATE SET
            platform_customer_id = EXCLUDED.platform_customer_id,
            last_order_date = EXCLUDED.last_order_date,
            total_orders = EXCLUDED.total_orders,
            total_spent = EXCLUDED.total_spent,
            preferred_platform = EXCLUDED.preferred_platform
        """;

    private static final String COPY_SQL = """
        COPY tbl_customer (
            customer_id, customer_key, platform_customer_id, phone_hash, email_hash,
            gender, age_group, customer_segment, customer_tier, acquisition_channel,
            first_order_date, last_order_date, total_orders, total_spent, average_order_value,
            total_items_purchased, days_since_first_order, days_since_last_order,
            purchase_frequency_days, return_rate, cancellation_rate, cod_preference_rate,
            favorite_category, favorite_brand, preferred_payment_method, preferred_platform,
            primary_shipping_province, ships_to_multiple_provinces, loyalty_points,
            referral_count, is_referrer
        ) FROM STDIN WITH (FORMAT CSV, DELIMITER ',')
        """;

    /**
     * OPTIMIZED: Bulk upsert with COPY FROM fallback
     */
    public int bulkUpsert(List<Customer> customers) {
        if (customers == null || customers.isEmpty()) return 0;

        // Try COPY FROM first for performance
        try {
            return bulkInsertWithCopy(customers);
        } catch (Exception e) {
            log.warn("COPY FROM failed, using batch upsert: {}", e.getMessage());
            return executeBatchUpsert(customers);
        }
    }

    /**
     * Find existing customers by IDs
     */
    public Map<String, Customer> findByIds(Set<String> customerIds) {
        if (customerIds == null || customerIds.isEmpty()) return Map.of();

        String sql = "SELECT * FROM tbl_customer WHERE customer_id = ANY(?)";
        return jdbcTemplate.query(sql, customerRowMapper(), customerIds.toArray(new String[0]))
                .stream().collect(java.util.stream.Collectors.toMap(
                        Customer::getCustomerId, customer -> customer));
    }

    public boolean exists(String customerId) {
        return !jdbcTemplate.queryForList("SELECT 1 FROM tbl_customer WHERE customer_id = ?", customerId).isEmpty();
    }

    public long count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM tbl_customer", Long.class);
    }

    // === COPY FROM Implementation ===

    private int bulkInsertWithCopy(List<Customer> customers) throws Exception {
        log.info("Bulk inserting {} customers using COPY FROM", customers.size());

        String csvData = generateCsvData(customers);
        return jdbcTemplate.execute((Connection conn) -> {
            CopyManager copyManager = new CopyManager((BaseConnection) conn.unwrap(BaseConnection.class));
            try (StringReader reader = new StringReader(csvData)) {
                return (int) copyManager.copyIn(COPY_SQL, reader);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private String generateCsvData(List<Customer> customers) {
        return customers.stream()
                .map(customer -> CsvFormatter.joinCsvRow(
                        customer.getCustomerId(), customer.getCustomerKey(), customer.getPlatformCustomerId(),
                        customer.getPhoneHash(), customer.getEmailHash(), customer.getGender(),
                        customer.getAgeGroup(), customer.getCustomerSegment(), customer.getCustomerTier(),
                        customer.getAcquisitionChannel(), CsvFormatter.formatDateTime(customer.getFirstOrderDate()),
                        CsvFormatter.formatDateTime(customer.getLastOrderDate()), customer.getTotalOrders(),
                        customer.getTotalSpent(), customer.getAverageOrderValue(), customer.getTotalItemsPurchased(),
                        customer.getDaysSinceFirstOrder(), customer.getDaysSinceLastOrder(),
                        customer.getPurchaseFrequencyDays(), customer.getReturnRate(), customer.getCancellationRate(),
                        customer.getCodPreferenceRate(), customer.getFavoriteCategory(), customer.getFavoriteBrand(),
                        customer.getPreferredPaymentMethod(), customer.getPreferredPlatform(),
                        customer.getPrimaryShippingProvince(), CsvFormatter.formatBoolean(customer.getShipsToMultipleProvinces()),
                        customer.getLoyaltyPoints(), customer.getReferralCount(), CsvFormatter.formatBoolean(customer.getIsReferrer())
                ))
                .collect(java.util.stream.Collectors.joining("\n"));
    }

    private int executeBatchUpsert(List<Customer> customers) {
        log.info("Batch upserting {} customers", customers.size());
        return jdbcTemplate.batchUpdate(UPSERT_SQL, customers.stream()
                .map(this::mapToParams).toList()).length;
    }

    private Object[] mapToParams(Customer c) {
        return new Object[]{
                c.getCustomerId(), c.getCustomerKey(), c.getPlatformCustomerId(), c.getPhoneHash(),
                c.getEmailHash(), c.getGender(), c.getAgeGroup(), c.getCustomerSegment(),
                c.getCustomerTier(), c.getAcquisitionChannel(), c.getFirstOrderDate(), c.getLastOrderDate(),
                c.getTotalOrders(), c.getTotalSpent(), c.getAverageOrderValue(), c.getTotalItemsPurchased(),
                c.getDaysSinceFirstOrder(), c.getDaysSinceLastOrder(), c.getPurchaseFrequencyDays(),
                c.getReturnRate(), c.getCancellationRate(), c.getCodPreferenceRate(), c.getFavoriteCategory(),
                c.getFavoriteBrand(), c.getPreferredPaymentMethod(), c.getPreferredPlatform(),
                c.getPrimaryShippingProvince(), c.getShipsToMultipleProvinces(), c.getLoyaltyPoints(),
                c.getReferralCount(), c.getIsReferrer()
        };
    }

    private RowMapper<Customer> customerRowMapper() {
        return (rs, rowNum) -> Customer.builder()
                .customerId(rs.getString("customer_id"))
                .customerKey(rs.getLong("customer_key"))
                .platformCustomerId(rs.getString("platform_customer_id"))
                .phoneHash(rs.getString("phone_hash"))
                .emailHash(rs.getString("email_hash"))
                .gender(rs.getString("gender"))
                .ageGroup(rs.getString("age_group"))
                .customerSegment(rs.getString("customer_segment"))
                .customerTier(rs.getString("customer_tier"))
                .acquisitionChannel(rs.getString("acquisition_channel"))
                .firstOrderDate(rs.getTimestamp("first_order_date") != null ?
                        rs.getTimestamp("first_order_date").toLocalDateTime() : null)
                .lastOrderDate(rs.getTimestamp("last_order_date") != null ?
                        rs.getTimestamp("last_order_date").toLocalDateTime() : null)
                .totalOrders(rs.getInt("total_orders"))
                .totalSpent(rs.getDouble("total_spent"))
                .averageOrderValue(rs.getDouble("average_order_value"))
                .totalItemsPurchased(rs.getInt("total_items_purchased"))
                .daysSinceFirstOrder(rs.getInt("days_since_first_order"))
                .daysSinceLastOrder(rs.getInt("days_since_last_order"))
                .purchaseFrequencyDays(rs.getDouble("purchase_frequency_days"))
                .returnRate(rs.getDouble("return_rate"))
                .cancellationRate(rs.getDouble("cancellation_rate"))
                .codPreferenceRate(rs.getDouble("cod_preference_rate"))
                .favoriteCategory(rs.getString("favorite_category"))
                .favoriteBrand(rs.getString("favorite_brand"))
                .preferredPaymentMethod(rs.getString("preferred_payment_method"))
                .preferredPlatform(rs.getString("preferred_platform"))
                .primaryShippingProvince(rs.getString("primary_shipping_province"))
                .shipsToMultipleProvinces(rs.getBoolean("ships_to_multiple_provinces"))
                .loyaltyPoints(rs.getInt("loyalty_points"))
                .referralCount(rs.getInt("referral_count"))
                .isReferrer(rs.getBoolean("is_referrer"))
                .build();
    }
}