package com.guno.dataimport.util;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * CSV Formatter Utility - Handle CSV formatting for PostgreSQL COPY FROM
 */
@UtilityClass
@Slf4j
public class CsvFormatter {

    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Escape value for CSV format
     */
    public String escape(Object value) {
        if (value == null) return "";

        String str = value.toString();
        if (str.contains(",") || str.contains("\"") || str.contains("\n") || str.contains("\r")) {
            return "\"" + str.replace("\"", "\"\"") + "\"";
        }
        return str;
    }

    /**
     * Format LocalDateTime for CSV
     */
    public String formatDateTime(LocalDateTime dateTime) {
        if (dateTime == null) return "";
        return dateTime.format(TIMESTAMP_FORMAT);
    }

    /**
     * Format boolean for CSV (PostgreSQL expects t/f)
     */
    public String formatBoolean(Boolean value) {
        if (value == null) return "";
        return value ? "t" : "f";
    }

    /**
     * Format number for CSV
     */
    public String formatNumber(Number value) {
        if (value == null) return "";
        return value.toString();
    }

    /**
     * Join CSV row values
     */
    public String joinCsvRow(Object... values) {
        StringBuilder row = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) row.append(",");
            row.append(escape(values[i]));
        }
        return row.toString();
    }
}