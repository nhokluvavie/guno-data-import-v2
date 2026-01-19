package com.guno.dataimport.test.util;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Date Range Summary
 * Tracks processing results across multiple dates
 */
@Data
@Slf4j
public class DateRangeSummary {

    private final String platform;
    private final List<String> processedDates = new ArrayList<>();
    private final Map<String, DateResult> results = new HashMap<>();

    private long totalOrders = 0;
    private long totalSuccess = 0;
    private long totalFailed = 0;
    private long totalProcessingTimeMs = 0;
    private int successfulDates = 0;
    private int failedDates = 0;

    public DateRangeSummary(String platform) {
        this.platform = platform;
    }

    /**
     * Add result for a specific date
     */
    public void addDateResult(String date, int ordersCollected, int successCount, int failedCount,
                              long processingTimeMs, boolean success, String errorMessage) {
        processedDates.add(date);

        DateResult result = new DateResult();
        result.setDate(date);
        result.setOrdersCollected(ordersCollected);
        result.setSuccessCount(successCount);
        result.setFailedCount(failedCount);
        result.setProcessingTimeMs(processingTimeMs);
        result.setSuccess(success);
        result.setErrorMessage(errorMessage);

        results.put(date, result);

        // Update totals
        totalOrders += ordersCollected;
        totalSuccess += successCount;
        totalFailed += failedCount;
        totalProcessingTimeMs += processingTimeMs;

        if (success) {
            successfulDates++;
        } else {
            failedDates++;
        }
    }

    /**
     * Print comprehensive summary
     */
    public void printSummary() {
        log.info("╔═══════════════════════════════════════════════════════════════════╗");
        log.info("║  📊 {} DATE RANGE PROCESSING SUMMARY", platform.toUpperCase());
        log.info("╠═══════════════════════════════════════════════════════════════════╣");
        log.info("║  Total Dates Processed: {}", processedDates.size());
        log.info("║  Successful: {} | Failed: {}", successfulDates, failedDates);
        log.info("║  Success Rate: {:.1f}%", getOverallSuccessRate());
        log.info("╠═══════════════════════════════════════════════════════════════════╣");
        log.info("║  📦 ORDERS");
        log.info("║  Total Collected: {}", totalOrders);
        log.info("║  Successfully Processed: {}", totalSuccess);
        log.info("║  Failed: {}", totalFailed);
        log.info("║  Processing Success Rate: {:.1f}%", getProcessingSuccessRate());
        log.info("╠═══════════════════════════════════════════════════════════════════╣");
        log.info("║  ⏱️ PERFORMANCE");
        log.info("║  Total Processing Time: {}ms ({} minutes)",
                totalProcessingTimeMs, totalProcessingTimeMs / 60000);
        log.info("║  Average Time per Date: {}ms", getAverageTimePerDate());
        log.info("║  Average Time per Order: {}ms", getAverageTimePerOrder());
        log.info("╠═══════════════════════════════════════════════════════════════════╣");
        log.info("║  📅 PER-DATE BREAKDOWN");

        for (String date : processedDates) {
            DateResult result = results.get(date);
            String status = result.isSuccess() ? "✅" : "❌";
            log.info("║  {} {} | Orders: {} | Success: {} | Failed: {} | Time: {}ms",
                    status, date, result.getOrdersCollected(), result.getSuccessCount(),
                    result.getFailedCount(), result.getProcessingTimeMs());

            if (!result.isSuccess() && result.getErrorMessage() != null) {
                log.info("║     Error: {}", result.getErrorMessage());
            }
        }

        log.info("╚═══════════════════════════════════════════════════════════════════╝");
    }

    /**
     * Get overall success rate (dates)
     */
    public double getOverallSuccessRate() {
        if (processedDates.isEmpty()) return 0.0;
        return (successfulDates * 100.0) / processedDates.size();
    }

    /**
     * Get processing success rate (orders)
     */
    public double getProcessingSuccessRate() {
        if (totalOrders == 0) return 0.0;
        return (totalSuccess * 100.0) / (totalSuccess + totalFailed);
    }

    /**
     * Get average time per date
     */
    public long getAverageTimePerDate() {
        if (processedDates.isEmpty()) return 0;
        return totalProcessingTimeMs / processedDates.size();
    }

    /**
     * Get average time per order
     */
    public long getAverageTimePerOrder() {
        if (totalOrders == 0) return 0;
        return totalProcessingTimeMs / totalOrders;
    }

    /**
     * Check if all dates succeeded
     */
    public boolean isAllSuccessful() {
        return failedDates == 0;
    }

    /**
     * Get failed dates
     */
    public List<String> getFailedDates() {
        List<String> failed = new ArrayList<>();
        for (Map.Entry<String, DateResult> entry : results.entrySet()) {
            if (!entry.getValue().isSuccess()) {
                failed.add(entry.getKey());
            }
        }
        return failed;
    }

    /**
     * Inner class for date result
     */
    @Data
    public static class DateResult {
        private String date;
        private int ordersCollected;
        private int successCount;
        private int failedCount;
        private long processingTimeMs;
        private boolean success;
        private String errorMessage;

        public double getSuccessRate() {
            if (ordersCollected == 0) return 0.0;
            return (successCount * 100.0) / ordersCollected;
        }
    }
}