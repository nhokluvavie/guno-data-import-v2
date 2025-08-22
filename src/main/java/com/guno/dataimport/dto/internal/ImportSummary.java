// =============================================
// Complete ImportSummary.java with all missing methods
// =============================================

package com.guno.dataimport.dto.internal;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

/**
 * Import Summary - Complete result of data import operation
 * ENHANCED: Now supports multi-platform operations with merge capability
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ImportSummary {

    @Builder.Default
    private LocalDateTime startTime = LocalDateTime.now();

    private LocalDateTime endTime;

    @Builder.Default
    private String status = "IN_PROGRESS";

    private String errorMessage;

    @Builder.Default
    private Integer totalApiCalls = 0;

    @Builder.Default
    private Integer totalDbOperations = 0;

    private Long processingTimeMs;

    @Builder.Default
    private Map<String, Integer> platformCounts = new HashMap<>();

    @Builder.Default
    private Map<String, Integer> tableInsertCounts = new HashMap<>();

    // =============================================
    // MISSING METHODS - Add these to ImportSummary.java
    // =============================================

    /**
     * Merge another ImportSummary into this one (for multi-platform processing)
     */
    public void merge(ImportSummary other) {
        if (other == null) return;

        // Merge basic counts
        this.totalApiCalls += other.totalApiCalls != null ? other.totalApiCalls : 0;
        this.totalDbOperations += other.totalDbOperations != null ? other.totalDbOperations : 0;

        // Merge platform counts
        if (other.platformCounts != null) {
            other.platformCounts.forEach((platform, count) -> {
                this.platformCounts.merge(platform, count, Integer::sum);
            });
        }

        // Merge table insert counts
        if (other.tableInsertCounts != null) {
            other.tableInsertCounts.forEach((table, count) -> {
                this.tableInsertCounts.merge(table, count, Integer::sum);
            });
        }

        // Update processing time if both have it
        if (other.processingTimeMs != null) {
            this.processingTimeMs = (this.processingTimeMs != null ? this.processingTimeMs : 0) +
                    other.processingTimeMs;
        }

        // Keep the earliest start time
        if (other.startTime != null &&
                (this.startTime == null || other.startTime.isBefore(this.startTime))) {
            this.startTime = other.startTime;
        }

        // Keep the latest end time
        if (other.endTime != null &&
                (this.endTime == null || other.endTime.isAfter(this.endTime))) {
            this.endTime = other.endTime;
        }

        // Merge status (fail if any failed)
        if ("FAILED".equals(other.status)) {
            this.status = "FAILED";
            if (other.errorMessage != null) {
                this.errorMessage = (this.errorMessage != null ? this.errorMessage + "; " : "") +
                        other.errorMessage;
            }
        }
    }

    /**
     * Get count for specific platform
     */
    public Integer getPlatformCount(String platform) {
        return platformCounts.getOrDefault(platform, 0);
    }

    /**
     * Add count for specific platform
     */
    public void addPlatformCount(String platform, Integer count) {
        if (count != null && count > 0) {
            platformCounts.merge(platform, count, Integer::sum);
        }
    }

    /**
     * Add count for specific table
     */
    public void addTableInsertCount(String table, Integer count) {
        if (count != null && count > 0) {
            tableInsertCounts.merge(table, count, Integer::sum);
        }
    }

    /**
     * Mark import as failed
     */
    public void markFailed(String errorMessage) {
        this.status = "FAILED";
        this.errorMessage = errorMessage;
        if (this.endTime == null) {
            this.endTime = LocalDateTime.now();
        }
    }

    /**
     * Mark import as successful
     */
    public void markSuccess() {
        this.status = "SUCCESS";
        if (this.endTime == null) {
            this.endTime = LocalDateTime.now();
        }
    }

    /**
     * Get formatted duration string
     */
    public String getDurationFormatted() {
        if (startTime == null) return "Unknown";

        LocalDateTime end = endTime != null ? endTime : LocalDateTime.now();
        long seconds = ChronoUnit.SECONDS.between(startTime, end);

        if (seconds < 60) {
            return seconds + "s";
        } else if (seconds < 3600) {
            return (seconds / 60) + "m " + (seconds % 60) + "s";
        } else {
            return (seconds / 3600) + "h " + ((seconds % 3600) / 60) + "m";
        }
    }

    /**
     * Get duration in milliseconds
     */
    public Long getDurationMs() {
        if (startTime == null) return null;

        LocalDateTime end = endTime != null ? endTime : LocalDateTime.now();
        return ChronoUnit.MILLIS.between(startTime, end);
    }

    /**
     * Check if import was successful
     */
    public boolean isSuccess() {
        return "SUCCESS".equals(status);
    }

    /**
     * Check if import failed
     */
    public boolean isFailed() {
        return "FAILED".equals(status);
    }

    /**
     * Check if import is still in progress
     */
    public boolean isInProgress() {
        return "IN_PROGRESS".equals(status);
    }

    /**
     * Get total orders processed across all platforms
     */
    public Integer getTotalOrdersProcessed() {
        return platformCounts.values().stream().mapToInt(Integer::intValue).sum();
    }

    /**
     * Get total table inserts across all tables
     */
    public Integer getTotalTableInserts() {
        return tableInsertCounts.values().stream().mapToInt(Integer::intValue).sum();
    }

    /**
     * Create ImportSummary with default table structure
     */
    public static ImportSummary createWithDefaultTables() {
        Map<String, Integer> defaultTables = new HashMap<>();
        defaultTables.put("customers", 0);
        defaultTables.put("orders", 0);
        defaultTables.put("order_items", 0);
        defaultTables.put("products", 0);
        defaultTables.put("geography_info", 0);
        defaultTables.put("payment_info", 0);
        defaultTables.put("shipping_info", 0);
        defaultTables.put("processing_date_info", 0);
        defaultTables.put("order_status", 0);
        defaultTables.put("order_status_detail", 0);
        defaultTables.put("status", 0);

        return ImportSummary.builder()
                .startTime(LocalDateTime.now())
                .status("IN_PROGRESS")
                .totalApiCalls(0)
                .totalDbOperations(0)
                .platformCounts(new HashMap<>())
                .tableInsertCounts(defaultTables)
                .build();
    }

    /**
     * Get summary statistics as formatted string
     */
    public String getSummaryStats() {
        return String.format(
                "ImportSummary[Status=%s, Duration=%s, Orders=%d, APIs=%d, DBOps=%d, Platforms=%s]",
                status,
                getDurationFormatted(),
                getTotalOrdersProcessed(),
                totalApiCalls,
                totalDbOperations,
                platformCounts.keySet()
        );
    }

    /**
     * Get detailed breakdown as formatted string
     */
    public String getDetailedBreakdown() {
        StringBuilder sb = new StringBuilder();
        sb.append("=== Import Summary Details ===\n");
        sb.append(String.format("Status: %s\n", status));
        sb.append(String.format("Duration: %s\n", getDurationFormatted()));
        sb.append(String.format("Start Time: %s\n", startTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)));
        if (endTime != null) {
            sb.append(String.format("End Time: %s\n", endTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)));
        }

        sb.append("\n--- Platform Counts ---\n");
        platformCounts.forEach((platform, count) ->
                sb.append(String.format("%s: %d orders\n", platform, count)));

        sb.append("\n--- Performance Metrics ---\n");
        sb.append(String.format("Total API Calls: %d\n", totalApiCalls));
        sb.append(String.format("Total DB Operations: %d\n", totalDbOperations));
        sb.append(String.format("Total Orders: %d\n", getTotalOrdersProcessed()));

        if (errorMessage != null) {
            sb.append(String.format("\n--- Error ---\n%s\n", errorMessage));
        }

        return sb.toString();
    }
}