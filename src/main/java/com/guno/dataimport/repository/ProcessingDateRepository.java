package com.guno.dataimport.repository;

import com.guno.dataimport.entity.ProcessingDateInfo;
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
 * ProcessingDate Repository - JDBC operations with COPY FROM optimization
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class ProcessingDateRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final String UPSERT_SQL = """
        INSERT INTO tbl_processing_date_info (
            order_id, date_key, full_date, day_of_week, day_of_week_name, day_of_month,
            day_of_year, week_of_year, month_of_year, month_name, quarter_of_year,
            quarter_name, year, is_weekend, is_holiday, holiday_name, is_business_day,
            fiscal_year, fiscal_quarter, is_shopping_season, season_name, is_peak_hour
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (order_id) DO UPDATE SET
            full_date = EXCLUDED.full_date,
            day_of_week = EXCLUDED.day_of_week,
            day_of_week_name = EXCLUDED.day_of_week_name,
            is_weekend = EXCLUDED.is_weekend,
            is_holiday = EXCLUDED.is_holiday,
            is_business_day = EXCLUDED.is_business_day,
            is_shopping_season = EXCLUDED.is_shopping_season
        """;

    private static final String COPY_SQL = """
        COPY tbl_processing_date_info (
            order_id, date_key, full_date, day_of_week, day_of_week_name, day_of_month,
            day_of_year, week_of_year, month_of_year, month_name, quarter_of_year,
            quarter_name, year, is_weekend, is_holiday, holiday_name, is_business_day,
            fiscal_year, fiscal_quarter, is_shopping_season, season_name, is_peak_hour
        ) FROM STDIN WITH (FORMAT CSV, DELIMITER ',')
        """;

    /**
     * OPTIMIZED: Bulk upsert with COPY FROM fallback
     */
    public int bulkUpsert(List<ProcessingDateInfo> dateInfos) {
        if (dateInfos == null || dateInfos.isEmpty()) return 0;

        try {
            return bulkInsertWithCopy(dateInfos);
        } catch (Exception e) {
            log.warn("COPY FROM failed, using batch upsert: {}", e.getMessage());
            return executeBatchUpsert(dateInfos);
        }
    }

    public List<ProcessingDateInfo> findByOrderIds(Set<String> orderIds) {
        if (orderIds == null || orderIds.isEmpty()) return List.of();

        String sql = "SELECT * FROM tbl_processing_date_info WHERE order_id = ANY(?)";
        return jdbcTemplate.query(sql, dateInfoRowMapper(), orderIds.toArray(new String[0]));
    }

    public ProcessingDateInfo findByOrderId(String orderId) {
        String sql = "SELECT * FROM tbl_processing_date_info WHERE order_id = ?";
        List<ProcessingDateInfo> results = jdbcTemplate.query(sql, dateInfoRowMapper(), orderId);
        return results.isEmpty() ? null : results.get(0);
    }

    public int deleteByOrderIds(Set<String> orderIds) {
        if (orderIds == null || orderIds.isEmpty()) return 0;

        String sql = "DELETE FROM tbl_processing_date_info WHERE order_id = ANY(?)";
        return jdbcTemplate.update(sql, orderIds.toArray(new String[0]));
    }

    public long count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM tbl_processing_date_info", Long.class);
    }

    // === COPY FROM Implementation ===

    private int bulkInsertWithCopy(List<ProcessingDateInfo> dateInfos) throws Exception {
        log.info("Bulk inserting {} processing date records using COPY FROM", dateInfos.size());

        String csvData = generateCsvData(dateInfos);
        return jdbcTemplate.execute((Connection conn) -> {
            CopyManager copyManager = new CopyManager((BaseConnection) conn.unwrap(BaseConnection.class));
            try (StringReader reader = new StringReader(csvData)) {
                return (int) copyManager.copyIn(COPY_SQL, reader);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private String generateCsvData(List<ProcessingDateInfo> dateInfos) {
        return dateInfos.stream()
                .map(dateInfo -> CsvFormatter.joinCsvRow(
                        dateInfo.getOrderId(), dateInfo.getDateKey(), CsvFormatter.formatDateTime(dateInfo.getFullDate()),
                        dateInfo.getDayOfWeek(), dateInfo.getDayOfWeekName(), dateInfo.getDayOfMonth(),
                        dateInfo.getDayOfYear(), dateInfo.getWeekOfYear(), dateInfo.getMonthOfYear(),
                        dateInfo.getMonthName(), dateInfo.getQuarterOfYear(), dateInfo.getQuarterName(),
                        dateInfo.getYear(), CsvFormatter.formatBoolean(dateInfo.getIsWeekend()),
                        CsvFormatter.formatBoolean(dateInfo.getIsHoliday()), dateInfo.getHolidayName(),
                        CsvFormatter.formatBoolean(dateInfo.getIsBusinessDay()), dateInfo.getFiscalYear(),
                        dateInfo.getFiscalQuarter(), CsvFormatter.formatBoolean(dateInfo.getIsShoppingSeason()),
                        dateInfo.getSeasonName(), CsvFormatter.formatBoolean(dateInfo.getIsPeakHour())
                ))
                .collect(java.util.stream.Collectors.joining("\n"));
    }

    private int executeBatchUpsert(List<ProcessingDateInfo> dateInfos) {
        log.info("Batch upserting {} processing date records", dateInfos.size());
        return jdbcTemplate.batchUpdate(UPSERT_SQL, dateInfos.stream()
                .map(this::mapToParams).toList()).length;
    }

    private Object[] mapToParams(ProcessingDateInfo d) {
        return new Object[]{
                d.getOrderId(), d.getDateKey(), d.getFullDate(), d.getDayOfWeek(), d.getDayOfWeekName(),
                d.getDayOfMonth(), d.getDayOfYear(), d.getWeekOfYear(), d.getMonthOfYear(),
                d.getMonthName(), d.getQuarterOfYear(), d.getQuarterName(), d.getYear(),
                d.getIsWeekend(), d.getIsHoliday(), d.getHolidayName(), d.getIsBusinessDay(),
                d.getFiscalYear(), d.getFiscalQuarter(), d.getIsShoppingSeason(), d.getSeasonName(), d.getIsPeakHour()
        };
    }

    private RowMapper<ProcessingDateInfo> dateInfoRowMapper() {
        return (rs, rowNum) -> ProcessingDateInfo.builder()
                .orderId(rs.getString("order_id"))
                .dateKey(rs.getLong("date_key"))
                .fullDate(rs.getTimestamp("full_date") != null ?
                        rs.getTimestamp("full_date").toLocalDateTime() : null)
                .dayOfWeek(rs.getInt("day_of_week"))
                .dayOfWeekName(rs.getString("day_of_week_name"))
                .dayOfMonth(rs.getInt("day_of_month"))
                .dayOfYear(rs.getInt("day_of_year"))
                .weekOfYear(rs.getInt("week_of_year"))
                .monthOfYear(rs.getInt("month_of_year"))
                .monthName(rs.getString("month_name"))
                .quarterOfYear(rs.getInt("quarter_of_year"))
                .quarterName(rs.getString("quarter_name"))
                .year(rs.getInt("year"))
                .isWeekend(rs.getBoolean("is_weekend"))
                .isHoliday(rs.getBoolean("is_holiday"))
                .holidayName(rs.getString("holiday_name"))
                .isBusinessDay(rs.getBoolean("is_business_day"))
                .fiscalYear(rs.getInt("fiscal_year"))
                .fiscalQuarter(rs.getInt("fiscal_quarter"))
                .isShoppingSeason(rs.getBoolean("is_shopping_season"))
                .seasonName(rs.getString("season_name"))
                .isPeakHour(rs.getBoolean("is_peak_hour"))
                .build();
    }
}