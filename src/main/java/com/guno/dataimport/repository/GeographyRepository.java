package com.guno.dataimport.repository;

import com.guno.dataimport.entity.GeographyInfo;
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
 * Geography Repository - JDBC operations with COPY FROM optimization
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

    private static final String COPY_SQL = """
        COPY tbl_geography_info (
            order_id, geography_key, country_code, country_name, region_code, region_name,
            province_code, province_name, province_type, district_code, district_name,
            district_type, ward_code, ward_name, ward_type, is_urban, is_metropolitan,
            is_coastal, is_border, economic_tier, population_density, income_level,
            shipping_zone, delivery_complexity, standard_delivery_days,
            express_delivery_available, latitude, longitude
        ) FROM STDIN WITH (FORMAT CSV, DELIMITER ',')
        """;

    /**
     * OPTIMIZED: Bulk upsert with COPY FROM fallback
     */
    public int bulkUpsert(List<GeographyInfo> geographyInfos) {
        if (geographyInfos == null || geographyInfos.isEmpty()) return 0;
        try {
            return tempTableUpsert(geographyInfos, "tbl_geography_info",
                    "order_id", "province_name = EXCLUDED.province_name, shipping_zone = EXCLUDED.shipping_zone");
        } catch (Exception e) {
            log.warn("Temp table failed, using batch: {}", e.getMessage());
            return executeBatchUpsert(geographyInfos);
        }
    }


    public List<GeographyInfo> findByOrderIds(Set<String> orderIds) {
        if (orderIds == null || orderIds.isEmpty()) return List.of();

        String sql = "SELECT * FROM tbl_geography_info WHERE order_id = ANY(?)";
        return jdbcTemplate.query(sql, geographyRowMapper(), orderIds.toArray(new String[0]));
    }

    public GeographyInfo findByOrderId(String orderId) {
        String sql = "SELECT * FROM tbl_geography_info WHERE order_id = ?";
        List<GeographyInfo> results = jdbcTemplate.query(sql, geographyRowMapper(), orderId);
        return results.isEmpty() ? null : results.get(0);
    }

    public long count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM tbl_geography_info", Long.class);
    }

    // === COPY FROM Implementation ===

    public int bulkInsertWithCopy(List<GeographyInfo> geographyInfos) throws Exception {
        log.info("Bulk inserting {} geography records using COPY FROM", geographyInfos.size());

        String csvData = generateCsvData(geographyInfos);
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
            String sql = "DELETE FROM tbl_geography_info WHERE order_id IN (" + placeholders + ")";
            return jdbcTemplate.update(sql, orderIds.toArray());
        }

        List<String> idList = new ArrayList<>(orderIds);
        int totalDeleted = 0;
        for (int i = 0; i < idList.size(); i += 1000) {
            List<String> batch = idList.subList(i, Math.min(i + 1000, idList.size()));
            String placeholders = String.join(",", Collections.nCopies(batch.size(), "?"));
            String sql = "DELETE FROM tbl_geography_info WHERE order_id IN (" + placeholders + ")";
            totalDeleted += jdbcTemplate.update(sql, batch.toArray());
        }
        return totalDeleted;
    }

    private <T> int tempTableUpsert(List<GeographyInfo> entities, String tableName, String conflictColumns, String updateSet) throws Exception {
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

    private String generateCsvData(List<GeographyInfo> geographyInfos) {
        return geographyInfos.stream()
                .map(geography -> CsvFormatter.joinCsvRow(
                        geography.getOrderId(), geography.getGeographyKey(), geography.getCountryCode(),
                        geography.getCountryName(), geography.getRegionCode(), geography.getRegionName(),
                        geography.getProvinceCode(), geography.getProvinceName(), geography.getProvinceType(),
                        geography.getDistrictCode(), geography.getDistrictName(), geography.getDistrictType(),
                        geography.getWardCode(), geography.getWardName(), geography.getWardType(),
                        CsvFormatter.formatBoolean(geography.getIsUrban()), CsvFormatter.formatBoolean(geography.getIsMetropolitan()),
                        CsvFormatter.formatBoolean(geography.getIsCoastal()), CsvFormatter.formatBoolean(geography.getIsBorder()),
                        geography.getEconomicTier(), geography.getPopulationDensity(), geography.getIncomeLevel(),
                        geography.getShippingZone(), geography.getDeliveryComplexity(), geography.getStandardDeliveryDays(),
                        CsvFormatter.formatBoolean(geography.getExpressDeliveryAvailable()), geography.getLatitude(), geography.getLongitude()
                ))
                .collect(java.util.stream.Collectors.joining("\n"));
    }

    private int executeBatchUpsert(List<GeographyInfo> geographyInfos) {
        log.info("Batch upserting {} geography records", geographyInfos.size());
        return jdbcTemplate.batchUpdate(UPSERT_SQL, geographyInfos.stream()
                .map(this::mapToParams).toList()).length;
    }

    private Object[] mapToParams(GeographyInfo g) {
        return new Object[]{
                g.getOrderId(), g.getGeographyKey(), g.getCountryCode(), g.getCountryName(),
                g.getRegionCode(), g.getRegionName(), g.getProvinceCode(), g.getProvinceName(),
                g.getProvinceType(), g.getDistrictCode(), g.getDistrictName(), g.getDistrictType(),
                g.getWardCode(), g.getWardName(), g.getWardType(), g.getIsUrban(), g.getIsMetropolitan(),
                g.getIsCoastal(), g.getIsBorder(), g.getEconomicTier(), g.getPopulationDensity(),
                g.getIncomeLevel(), g.getShippingZone(), g.getDeliveryComplexity(),
                g.getStandardDeliveryDays(), g.getExpressDeliveryAvailable(), g.getLatitude(), g.getLongitude()
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