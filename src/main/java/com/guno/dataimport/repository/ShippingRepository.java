package com.guno.dataimport.repository;

import com.guno.dataimport.entity.ShippingInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.Set;

/**
 * Shipping Repository - JDBC operations for ShippingInfo entity
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class ShippingRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final String UPSERT_SQL = """
        INSERT INTO tbl_shipping_info (
            order_id, shipping_key, provider_id, provider_name, provider_type, provider_tier,
            service_type, service_tier, delivery_commitment, shipping_method, pickup_type,
            delivery_type, base_fee, weight_based_fee, distance_based_fee, cod_fee,
            insurance_fee, supports_cod, supports_insurance, supports_fragile,
            supports_refrigerated, provides_tracking, provides_sms_updates,
            average_delivery_days, on_time_delivery_rate, success_delivery_rate,
            damage_rate, coverage_provinces, coverage_nationwide, coverage_international
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (order_id) DO UPDATE SET
            provider_name = EXCLUDED.provider_name,
            service_type = EXCLUDED.service_type,
            delivery_commitment = EXCLUDED.delivery_commitment,
            shipping_method = EXCLUDED.shipping_method,
            base_fee = EXCLUDED.base_fee,
            weight_based_fee = EXCLUDED.weight_based_fee,
            distance_based_fee = EXCLUDED.distance_based_fee,
            provides_tracking = EXCLUDED.provides_tracking
        """;

    // Bulk upsert shipping info
    public int bulkUpsert(List<ShippingInfo> shippingInfos) {
        if (shippingInfos == null || shippingInfos.isEmpty()) {
            return 0;
        }

        log.info("Bulk upserting {} shipping records", shippingInfos.size());

        return jdbcTemplate.batchUpdate(UPSERT_SQL, shippingInfos.stream()
                .map(this::mapShippingToParams)
                .toList()
        ).length;
    }

    // Find shipping info by order IDs
    public List<ShippingInfo> findByOrderIds(Set<String> orderIds) {
        if (orderIds == null || orderIds.isEmpty()) {
            return List.of();
        }

        String sql = "SELECT * FROM tbl_shipping_info WHERE order_id = ANY(?)";
        return jdbcTemplate.query(sql, shippingRowMapper(),
                orderIds.toArray(new String[0]));
    }

    // Find by order ID
    public ShippingInfo findByOrderId(String orderId) {
        String sql = "SELECT * FROM tbl_shipping_info WHERE order_id = ?";
        List<ShippingInfo> results = jdbcTemplate.query(sql, shippingRowMapper(), orderId);
        return results.isEmpty() ? null : results.get(0);
    }

    // Delete by order IDs
    public int deleteByOrderIds(Set<String> orderIds) {
        if (orderIds == null || orderIds.isEmpty()) {
            return 0;
        }

        String sql = "DELETE FROM tbl_shipping_info WHERE order_id = ANY(?)";
        return jdbcTemplate.update(sql, orderIds.toArray(new String[0]));
    }

    // Get total count
    public long count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM tbl_shipping_info", Long.class);
    }

    // Helper methods
    private Object[] mapShippingToParams(ShippingInfo shipping) {
        return new Object[]{
                shipping.getOrderId(),
                shipping.getShippingKey(),
                shipping.getProviderId(),
                shipping.getProviderName(),
                shipping.getProviderType(),
                shipping.getProviderTier(),
                shipping.getServiceType(),
                shipping.getServiceTier(),
                shipping.getDeliveryCommitment(),
                shipping.getShippingMethod(),
                shipping.getPickupType(),
                shipping.getDeliveryType(),
                shipping.getBaseFee(),
                shipping.getWeightBasedFee(),
                shipping.getDistanceBasedFee(),
                shipping.getCodFee(),
                shipping.getInsuranceFee(),
                shipping.getSupportsCod(),
                shipping.getSupportsInsurance(),
                shipping.getSupportsFragile(),
                shipping.getSupportsRefrigerated(),
                shipping.getProvidesTracking(),
                shipping.getProvidesSmsUpdates(),
                shipping.getAverageDeliveryDays(),
                shipping.getOnTimeDeliveryRate(),
                shipping.getSuccessDeliveryRate(),
                shipping.getDamageRate(),
                shipping.getCoverageProvinces(),
                shipping.getCoverageNationwide(),
                shipping.getCoverageInternational()
        };
    }

    private RowMapper<ShippingInfo> shippingRowMapper() {
        return (rs, rowNum) -> ShippingInfo.builder()
                .orderId(rs.getString("order_id"))
                .shippingKey(rs.getLong("shipping_key"))
                .providerId(rs.getString("provider_id"))
                .providerName(rs.getString("provider_name"))
                .providerType(rs.getString("provider_type"))
                .providerTier(rs.getString("provider_tier"))
                .serviceType(rs.getString("service_type"))
                .serviceTier(rs.getString("service_tier"))
                .deliveryCommitment(rs.getString("delivery_commitment"))
                .shippingMethod(rs.getString("shipping_method"))
                .pickupType(rs.getString("pickup_type"))
                .deliveryType(rs.getString("delivery_type"))
                .baseFee(rs.getDouble("base_fee"))
                .weightBasedFee(rs.getDouble("weight_based_fee"))
                .distanceBasedFee(rs.getDouble("distance_based_fee"))
                .codFee(rs.getDouble("cod_fee"))
                .insuranceFee(rs.getDouble("insurance_fee"))
                .supportsCod(rs.getBoolean("supports_cod"))
                .supportsInsurance(rs.getBoolean("supports_insurance"))
                .supportsFragile(rs.getBoolean("supports_fragile"))
                .supportsRefrigerated(rs.getBoolean("supports_refrigerated"))
                .providesTracking(rs.getBoolean("provides_tracking"))
                .providesSmsUpdates(rs.getBoolean("provides_sms_updates"))
                .averageDeliveryDays(rs.getDouble("average_delivery_days"))
                .onTimeDeliveryRate(rs.getDouble("on_time_delivery_rate"))
                .successDeliveryRate(rs.getDouble("success_delivery_rate"))
                .damageRate(rs.getDouble("damage_rate"))
                .coverageProvinces(rs.getString("coverage_provinces"))
                .coverageNationwide(rs.getBoolean("coverage_nationwide"))
                .coverageInternational(rs.getBoolean("coverage_international"))
                .build();
    }
}