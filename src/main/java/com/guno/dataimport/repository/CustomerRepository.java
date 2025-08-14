package com.guno.dataimport.repository;

import com.guno.dataimport.entity.Customer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Customer Repository - JDBC operations for Customer entity
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class CustomerRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final String INSERT_SQL = """
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
        """;

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
            phone_hash = EXCLUDED.phone_hash,
            email_hash = EXCLUDED.email_hash,
            gender = EXCLUDED.gender,
            last_order_date = EXCLUDED.last_order_date,
            total_orders = EXCLUDED.total_orders,
            total_spent = EXCLUDED.total_spent,
            average_order_value = EXCLUDED.average_order_value,
            total_items_purchased = EXCLUDED.total_items_purchased,
            days_since_last_order = EXCLUDED.days_since_last_order,
            preferred_platform = EXCLUDED.preferred_platform
        """;

    // Bulk upsert customers
    public int bulkUpsert(List<Customer> customers) {
        if (customers == null || customers.isEmpty()) {
            return 0;
        }

        log.info("Bulk upserting {} customers", customers.size());

        return jdbcTemplate.batchUpdate(UPSERT_SQL, customers.stream()
                .map(this::mapCustomerToParams)
                .toList()
        ).length;
    }

    // Find existing customers by IDs
    public Map<String, Customer> findByIds(Set<String> customerIds) {
        if (customerIds == null || customerIds.isEmpty()) {
            return Map.of();
        }

        String sql = "SELECT * FROM tbl_customer WHERE customer_id = ANY(?)";
        List<Customer> customers = jdbcTemplate.query(sql, customerRowMapper(),
                customerIds.toArray(new String[0]));

        return customers.stream()
                .collect(java.util.stream.Collectors.toMap(
                        Customer::getCustomerId,
                        customer -> customer
                ));
    }

    // Check if customer exists
    public boolean exists(String customerId) {
        String sql = "SELECT 1 FROM tbl_customer WHERE customer_id = ?";
        return !jdbcTemplate.queryForList(sql, customerId).isEmpty();
    }

    // Get total customer count
    public long count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM tbl_customer", Long.class);
    }

    // Helper methods
    private Object[] mapCustomerToParams(Customer customer) {
        return new Object[]{
                customer.getCustomerId(),
                customer.getCustomerKey(),
                customer.getPlatformCustomerId(),
                customer.getPhoneHash(),
                customer.getEmailHash(),
                customer.getGender(),
                customer.getAgeGroup(),
                customer.getCustomerSegment(),
                customer.getCustomerTier(),
                customer.getAcquisitionChannel(),
                customer.getFirstOrderDate(),
                customer.getLastOrderDate(),
                customer.getTotalOrders(),
                customer.getTotalSpent(),
                customer.getAverageOrderValue(),
                customer.getTotalItemsPurchased(),
                customer.getDaysSinceFirstOrder(),
                customer.getDaysSinceLastOrder(),
                customer.getPurchaseFrequencyDays(),
                customer.getReturnRate(),
                customer.getCancellationRate(),
                customer.getCodPreferenceRate(),
                customer.getFavoriteCategory(),
                customer.getFavoriteBrand(),
                customer.getPreferredPaymentMethod(),
                customer.getPreferredPlatform(),
                customer.getPrimaryShippingProvince(),
                customer.getShipsToMultipleProvinces(),
                customer.getLoyaltyPoints(),
                customer.getReferralCount(),
                customer.getIsReferrer()
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