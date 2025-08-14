package com.guno.dataimport.repository;

import com.guno.dataimport.entity.ProcessingDateInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.Set;

/**
 * ProcessingDate Repository - JDBC operations for ProcessingDateInfo entity
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
            holiday_name = EXCLUDED.holiday_name,
            is_business_day = EXCLUDED.is_business_day,
            is_shopping_season = EXCLUDED.is_shopping_season,
            season_name = EXCLUDED.season_name,
            is_peak_hour = EXCLUDED.is_peak_hour
        """;

    // Bulk upsert processing date info
    public int bulkUpsert(List<ProcessingDateInfo> dateInfos) {
        if (dateInfos == null || dateInfos.isEmpty()) {
            return 0;
        }

        log.info("Bulk upserting {} processing date records", dateInfos.size());

        return jdbcTemplate.batchUpdate(UPSERT_SQL, dateInfos.stream()
                .map(this::mapDateInfoToParams)
                .toList()
        ).length;
    }

    // Find date info by order IDs
    public List<ProcessingDateInfo> findByOrderIds(Set<String> orderIds) {
        if (orderIds == null || orderIds.isEmpty()) {
            return List.of();
        }

        String sql = "SELECT * FROM tbl_processing_date_info WHERE order_id = ANY(?)";
        return jdbcTemplate.query(sql, dateInfoRowMapper(),
                orderIds.toArray(new String[0]));
    }

    // Find by order ID
    public ProcessingDateInfo findByOrderId(String orderId) {
        String sql = "SELECT * FROM tbl_processing_date_info WHERE order_id = ?";
        List<ProcessingDateInfo> results = jdbcTemplate.query(sql, dateInfoRowMapper(), orderId);
        return results.isEmpty() ? null : results.get(0);
    }

    // Delete by order IDs
    public int deleteByOrderIds(Set<String> orderIds) {
        if (orderIds == null || orderIds.isEmpty()) {
            return 0;
        }

        String sql = "DELETE FROM tbl_processing_date_info WHERE order_id = ANY(?)";
        return jdbcTemplate.update(sql, orderIds.toArray(new String[0]));
    }

    // Get total count
    public long count() {
        return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM tbl_processing_date_info", Long.class);
    }

    // Helper methods
    private Object[] mapDateInfoToParams(ProcessingDateInfo dateInfo) {
        return new Object[]{
                dateInfo.getOrderId(),
                dateInfo.getDateKey(),
                dateInfo.getFullDate(),
                dateInfo.getDayOfWeek(),
                dateInfo.getDayOfWeekName(),
                dateInfo.getDayOfMonth(),
                dateInfo.getDayOfYear(),
                dateInfo.getWeekOfYear(),
                dateInfo.getMonthOfYear(),
                dateInfo.getMonthName(),
                dateInfo.getQuarterOfYear(),
                dateInfo.getQuarterName(),
                dateInfo.getYear(),
                dateInfo.getIsWeekend(),
                dateInfo.getIsHoliday(),
                dateInfo.getHolidayName(),
                dateInfo.getIsBusinessDay(),
                dateInfo.getFiscalYear(),
                dateInfo.getFiscalQuarter(),
                dateInfo.getIsShoppingSeason(),
                dateInfo.getSeasonName(),
                dateInfo.getIsPeakHour()
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