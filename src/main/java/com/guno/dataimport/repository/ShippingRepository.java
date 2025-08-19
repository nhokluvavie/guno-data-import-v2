package com.guno.dataimport.repository;

import com.guno.dataimport.entity.ShippingInfo;
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
import java.util.Set;

/**
 * Shipping Repository - JDBC operations with COPY FROM optimization
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
            provides_tracking = EXCLUDED.provides_tracking
        """;

    private static final String COPY_SQL = """
        COPY tbl_shipping_info (
            order_id, shipping_key, provider_id, provider_name, provider_type, provider_tier,
            service_type, service_tier, delivery_commitment, shipping_method, pickup_type,
            delivery_type, base_fee, weight_based_fee, distance_based_fee, cod_fee,
            insurance_fee, supports_cod, supports_insurance, supports_fragile,
            supports_refrigerated, provides_tracking, provides_sms_updates,
            average_delivery_days, on_time_delivery_rate, success_delivery_rate,
            damage_rate, coverage_provinces, coverage_nationwide, coverage_international
        ) FROM STDIN WITH (FORMAT CSV, DELIMITER ',')
        """;

    /**
     * OPTIMIZED: Bulk upsert with COPY FROM fallback
     */
    public int bulkUpsert(List<ShippingInfo> shippingInfos) {
        if (shippingInfos == null || shippingInfos.isEmpty()) return 0;

        try {
            return bulkInsertWithCopy(shippingInfos);
        } catch (Exception e) {
            log.warn("COPY FROM failed, using batch upsert: {}", e.getMessage());
            return executeBatchUpsert(shippingInfos);
        }
    }

    public List<ShippingInfo> findByOrderIds(Set<String> orderIds) {
        if (orderIds == null || orderIds.isEmpty()) return List.of();

        String sql = "SELECT * FROM tbl_shipping_info WHERE order_id = ANY(?)";
        return jdbcTemplate.query(sql, shippingRowMapper(), orderIds.toArray(new String[0]));
    }

    public ShippingInfo findByOrderId(String orderId) {
        String sql = "SELECT * FROM tbl_shipping_info WHERE order_id = ?";
        List<ShippingInfo> results = jdbcTemplate.query(sql, shippingRowMapper(), orderId);
        return results.isEmpty() ? null : results.get(0);
    }

    public int deleteByOrderIds(Set<String> orderIds) {
        if (orderIds == null || orderIds.isEmpty()) return 0;

        String sql = "DELETE FROM tbl_shipping_info WHERE order_id = ANY(?)";
        return jdbcTemplate.update(sql, orderIds.toArray(new String[0]));
    }

    public long count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM tbl_shipping_info", Long.class);
    }

    // === COPY FROM Implementation ===

    private int bulkInsertWithCopy(List<ShippingInfo> shippingInfos) throws Exception {
        log.info("Bulk inserting {} shipping records using COPY FROM", shippingInfos.size());

        String csvData = generateCsvData(shippingInfos);
        return jdbcTemplate.execute((Connection conn) -> {
            CopyManager copyManager = new CopyManager((BaseConnection) conn.unwrap(BaseConnection.class));
            try (StringReader reader = new StringReader(csvData)) {
                return (int) copyManager.copyIn(COPY_SQL, reader);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private String generateCsvData(List<ShippingInfo> shippingInfos) {
        return shippingInfos.stream()
                .map(shipping -> CsvFormatter.joinCsvRow(
                        shipping.getOrderId(), shipping.getShippingKey(), shipping.getProviderId(),
                        shipping.getProviderName(), shipping.getProviderType(), shipping.getProviderTier(),
                        shipping.getServiceType(), shipping.getServiceTier(), shipping.getDeliveryCommitment(),
                        shipping.getShippingMethod(), shipping.getPickupType(), shipping.getDeliveryType(),
                        shipping.getBaseFee(), shipping.getWeightBasedFee(), shipping.getDistanceBasedFee(),
                        shipping.getCodFee(), shipping.getInsuranceFee(),
                        CsvFormatter.formatBoolean(shipping.getSupportsCod()), CsvFormatter.formatBoolean(shipping.getSupportsInsurance()),
                        CsvFormatter.formatBoolean(shipping.getSupportsFragile()), CsvFormatter.formatBoolean(shipping.getSupportsRefrigerated()),
                        CsvFormatter.formatBoolean(shipping.getProvidesTracking()), CsvFormatter.formatBoolean(shipping.getProvidesSmsUpdates()),
                        shipping.getAverageDeliveryDays(), shipping.getOnTimeDeliveryRate(),
                        shipping.getSuccessDeliveryRate(), shipping.getDamageRate(), shipping.getCoverageProvinces(),
                        CsvFormatter.formatBoolean(shipping.getCoverageNationwide()), CsvFormatter.formatBoolean(shipping.getCoverageInternational())
                ))
                .collect(java.util.stream.Collectors.joining("\n"));
    }

    private int executeBatchUpsert(List<ShippingInfo> shippingInfos) {
        log.info("Batch upserting {} shipping records", shippingInfos.size());
        return jdbcTemplate.batchUpdate(UPSERT_SQL, shippingInfos.stream()
                .map(this::mapToParams).toList()).length;
    }

    private Object[] mapToParams(ShippingInfo s) {
        return new Object[]{
                s.getOrderId(), s.getShippingKey(), s.getProviderId(), s.getProviderName(),
                s.getProviderType(), s.getProviderTier(), s.getServiceType(), s.getServiceTier(),
                s.getDeliveryCommitment(), s.getShippingMethod(), s.getPickupType(), s.getDeliveryType(),
                s.getBaseFee(), s.getWeightBasedFee(), s.getDistanceBasedFee(), s.getCodFee(),
                s.getInsuranceFee(), s.getSupportsCod(), s.getSupportsInsurance(), s.getSupportsFragile(),
                s.getSupportsRefrigerated(), s.getProvidesTracking(), s.getProvidesSmsUpdates(),
                s.getAverageDeliveryDays(), s.getOnTimeDeliveryRate(), s.getSuccessDeliveryRate(),
                s.getDamageRate(), s.getCoverageProvinces(), s.getCoverageNationwide(), s.getCoverageInternational()
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