package com.guno.dataimport.dto.internal;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Import Summary - Enhanced with table-level insert tracking
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ImportSummary {

    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private long duration;

    private int totalApiCalls;
    private int totalDbOperations;

    @Builder.Default
    private Map<String, Integer> platformCounts = new HashMap<>();

    @Builder.Default
    private Map<String, Integer> tableInsertCounts = new HashMap<>();

    private String status;
    private String errorMessage;

    /**
     * Get formatted duration string
     */
    public String getDurationFormatted() {
        if (duration < 1000) {
            return duration + "ms";
        } else {
            return String.format("%.1fs", duration / 1000.0);
        }
    }

    /**
     * Add insert count for specific table
     */
    public void addTableInsertCount(String tableName, int count) {
        tableInsertCounts.put(tableName, tableInsertCounts.getOrDefault(tableName, 0) + count);
    }

    /**
     * Set insert count for specific table
     */
    public void setTableInsertCount(String tableName, int count) {
        tableInsertCounts.put(tableName, count);
    }

    /**
     * Get insert count for specific table
     */
    public int getTableInsertCount(String tableName) {
        return tableInsertCounts.getOrDefault(tableName, 0);
    }

    /**
     * Get total records inserted across all tables
     */
    public int getTotalInsertedRecords() {
        return tableInsertCounts.values().stream().mapToInt(Integer::intValue).sum();
    }

    /**
     * Add platform count
     */
    public void addPlatformCount(String platform, int count) {
        platformCounts.put(platform, platformCounts.getOrDefault(platform, 0) + count);
    }

    /**
     * Calculate insert efficiency (records per API order)
     */
    public double getInsertEfficiency() {
        int totalApiOrders = platformCounts.values().stream().mapToInt(Integer::intValue).sum();
        if (totalApiOrders == 0) return 0.0;
        return (double) getTotalInsertedRecords() / totalApiOrders;
    }

    /**
     * Check if import was successful
     */
    public boolean isSuccess() {
        return "SUCCESS".equals(status) || errorMessage == null;
    }

    /**
     * Initialize table counts for all 11 tables
     */
    public static ImportSummary createWithDefaultTables() {
        ImportSummary summary = ImportSummary.builder()
                .startTime(LocalDateTime.now())
                .status("IN_PROGRESS")
                .build();

        // Initialize all 11 table counts to 0
        summary.setTableInsertCount("customers", 0);
        summary.setTableInsertCount("orders", 0);
        summary.setTableInsertCount("order_items", 0);
        summary.setTableInsertCount("products", 0);
        summary.setTableInsertCount("geography_info", 0);
        summary.setTableInsertCount("payment_info", 0);
        summary.setTableInsertCount("shipping_info", 0);
        summary.setTableInsertCount("processing_date_info", 0);
        summary.setTableInsertCount("order_status", 0);
        summary.setTableInsertCount("order_status_detail", 0);
        summary.setTableInsertCount("status", 0);

        return summary;
    }

    /**
     * Mark import as completed
     */
    public void markCompleted() {
        this.endTime = LocalDateTime.now();
        this.duration = java.time.Duration.between(startTime, endTime).toMillis();
        this.status = "SUCCESS";
    }

    /**
     * Mark import as failed
     */
    public void markFailed(String error) {
        this.endTime = LocalDateTime.now();
        this.duration = java.time.Duration.between(startTime, endTime).toMillis();
        this.status = "FAILED";
        this.errorMessage = error;
    }

    /**
     * Generate summary report
     */
    public String generateReport() {
        StringBuilder report = new StringBuilder();
        report.append("Import Summary Report\n");
        report.append("===================\n");
        report.append("Status: ").append(status).append("\n");
        report.append("Duration: ").append(getDurationFormatted()).append("\n");
        report.append("API Calls: ").append(totalApiCalls).append("\n");
        report.append("DB Operations: ").append(totalDbOperations).append("\n");

        report.append("\nPlatform Counts:\n");
        platformCounts.forEach((platform, count) ->
                report.append("  ").append(platform).append(": ").append(count).append("\n"));

        report.append("\nTable Insert Counts:\n");
        tableInsertCounts.forEach((table, count) ->
                report.append("  ").append(table).append(": ").append(count).append("\n"));

        report.append("\nTotal Inserted: ").append(getTotalInsertedRecords()).append("\n");
        report.append("Insert Efficiency: ").append(String.format("%.1f", getInsertEfficiency())).append(" records/order\n");

        return report.toString();
    }
}