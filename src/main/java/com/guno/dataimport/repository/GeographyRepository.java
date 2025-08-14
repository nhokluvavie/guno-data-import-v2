package com.guno.dataimport.repository;

import com.guno.dataimport.entity.GeographyInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.Set;

/**
 * Geography Repository - JDBC operations for GeographyInfo entity
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class GeographyRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final String UPSERT_SQL = """
        INSERT INTO tbl_geography_info (
            order_id, geography_key, country_code, country_name, region_code, region_name,
            province_code, province_name, province_type, district_code, district_name,
            district_type, ward_code, ward_name, ward_type, is_urban, is_metropolitan,
            is_coastal, is_border, economic_tier, population_density, income_level,
            shipping_zone, delivery_complexity, standard_delivery_days,
            express_delivery_available, latitude, longitude
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (order_id) DO UPDATE SET
            province_name = EXCLUDED.province_name,
            district_name = EXCLUDED.district_name,
            ward_name = EXCLUDED.ward_name,
            shipping_zone = EXCLUDED.shipping_zone,
            delivery_complexity = EXCLUDED.delivery_complexity,
            standard_delivery_days = EXCLUDED.standard_delivery_days,
            express_delivery_available = EXCLUDED.express_delivery_available
        """;

    // Bulk upsert geography info
    public int bulkUpsert(List<GeographyInfo> geographyInfos) {
        if (geographyInfos == null || geographyInfos.isEmpty()) {
            return 0;
        }

        log.info("Bulk upserting {} geography records", geographyInfos.size());

        return jdbcTemplate.batchUpdate(UPSERT_SQL, geographyInfos.stream()
                .map(this::mapGeographyToParams)
                .toList()
        ).length;
    }

    // Find geography info by order IDs
    public List<GeographyInfo> findByOrderIds(Set<String> orderIds) {
        if (orderIds == null || orderIds.isEmpty()) {
            return List.of();
        }

        String sql = "SELECT * FROM tbl_geography_info WHERE order_id = ANY(?)";
        return jdbcTemplate.query(sql, geographyRowMapper(),
                orderIds.toArray(new String[0]));
    }

    // Find by order ID
    public GeographyInfo findByOrderId(String orderId) {
        String sql = "SELECT * FROM tbl_geography_info WHERE order_id = ?";
        List<GeographyInfo> results = jdbcTemplate.query(sql, geographyRowMapper(), orderId);
        return results.isEmpty() ? null : results.get(0);
    }

    // Delete by order IDs
    public int deleteByOrderIds(Set<String> orderIds) {
        if (orderIds == null || orderIds.isEmpty()) {
            return 0;
        }

        String sql = "DELETE FROM tbl_geography_info WHERE order_id = ANY(?)";
        return jdbcTemplate.update(sql, orderIds.toArray(new String[0]));
    }

    // Get total count
    public long count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM tbl_geography_info", Long.class);
    }

    // Helper methods
    private Object[] mapGeographyToParams(GeographyInfo geography) {
        return new Object[]{
                geography.getOrderId(),
                geography.getGeographyKey(),
                geography.getCountryCode(),
                geography.getCountryName(),
                geography.getRegionCode(),
                geography.getRegionName(),
                geography.getProvinceCode(),
                geography.getProvinceName(),
                geography.getProvinceType(),
                geography.getDistrictCode(),
                geography.getDistrictName(),
                geography.getDistrictType(),
                geography.getWardCode(),
                geography.getWardName(),
                geography.getWardType(),
                geography.getIsUrban(),
                geography.getIsMetropolitan(),
                geography.getIsCoastal(),
                geography.getIsBorder(),
                geography.getEconomicTier(),
                geography.getPopulationDensity(),
                geography.getIncomeLevel(),
                geography.getShippingZone(),
                geography.getDeliveryComplexity(),
                geography.getStandardDeliveryDays(),
                geography.getExpressDeliveryAvailable(),
                geography.getLatitude(),
                geography.getLongitude()
        };
    }

    private RowMapper<GeographyInfo> geographyRowMapper() {
        return (rs, rowNum) -> GeographyInfo.builder()
                .orderId(rs.getString("order_id"))
                .geographyKey(rs.getLong("geography_key"))
                .countryCode(rs.getString("country_code"))
                .countryName(rs.getString("country_name"))
                .regionCode(rs.getString("region_code"))
                .regionName(rs.getString("region_name"))
                .provinceCode(rs.getString("province_code"))
                .provinceName(rs.getString("province_name"))
                .provinceType(rs.getString("province_type"))
                .districtCode(rs.getString("district_code"))
                .districtName(rs.getString("district_name"))
                .districtType(rs.getString("district_type"))
                .wardCode(rs.getString("ward_code"))
                .wardName(rs.getString("ward_name"))
                .wardType(rs.getString("ward_type"))
                .isUrban(rs.getBoolean("is_urban"))
                .isMetropolitan(rs.getBoolean("is_metropolitan"))
                .isCoastal(rs.getBoolean("is_coastal"))
                .isBorder(rs.getBoolean("is_border"))
                .economicTier(rs.getString("economic_tier"))
                .populationDensity(rs.getString("population_density"))
                .incomeLevel(rs.getString("income_level"))
                .shippingZone(rs.getString("shipping_zone"))
                .deliveryComplexity(rs.getString("delivery_complexity"))
                .standardDeliveryDays(rs.getInt("standard_delivery_days"))
                .expressDeliveryAvailable(rs.getBoolean("express_delivery_available"))
                .latitude(rs.getDouble("latitude"))
                .longitude(rs.getDouble("longitude"))
                .build();
    }
}