package com.guno.dataimport.buffer;

import com.guno.dataimport.api.client.FacebookApiClient;
import com.guno.dataimport.dto.internal.ImportSummary;
import com.guno.dataimport.dto.platform.facebook.FacebookApiResponse;
import com.guno.dataimport.dto.platform.facebook.FacebookOrderDto;
import com.guno.dataimport.processor.BatchProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * BufferedDataCollector - High-performance data collection with buffering
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class BufferedDataCollector {

    private final FacebookApiClient facebookApiClient;
    private final BatchProcessor batchProcessor;

    /**
     * Collect data with buffering for maximum performance
     * @param date Target date for API calls (from yml config)
     * @param bufferSize Number of orders to buffer before flush
     * @param pageSize Number of orders per API call
     * @return ImportSummary with detailed metrics (never null)
     */
    public ImportSummary collectWithBuffer(String date, int bufferSize, int pageSize) {
        log.info("🚀 Starting buffered collection - Date: '{}', Buffer: {}, PageSize: {}",
                date, bufferSize, pageSize);

        // ALWAYS create summary first - never return null
        ImportSummary summary = ImportSummary.createWithDefaultTables();

        if (summary == null) {
            log.error("❌ CRITICAL: ImportSummary.createWithDefaultTables() returned null!");
            summary = ImportSummary.builder()
                    .startTime(LocalDateTime.now())
                    .status("FAILED")
                    .errorMessage("Failed to create summary")
                    .platformCounts(new HashMap<>())
                    .tableInsertCounts(new HashMap<>())
                    .build();
        }

        List<Object> orderBuffer = new ArrayList<>(bufferSize);

        int currentPage = 1;
        int totalOrders = 0;
        int apiCalls = 0;
        boolean hasMoreData = true;

        try {
            while (hasMoreData) {
                log.debug("🔄 Calling API - Page: {}, Date: '{}'", currentPage, date);

                // Call Facebook API with configured date
                FacebookApiResponse response = facebookApiClient.fetchOrders(date, currentPage, pageSize);
                apiCalls++;

                if (response == null) {
                    log.warn("API call returned null response - Page: {}", currentPage);
                    break;
                }

                // Log detailed response info for debugging
                log.debug("API Response - Status: {}, Code: {}, Message: '{}'",
                        response.getStatus(), response.getCode(), response.getMessage());

                // Check if API call was successful (status 1 seems to be success for this API)
                if (response.getStatus() == null || response.getData() == null) {
                    log.warn("API call failed or no data - Page: {}, Status: {}, HasData: {}",
                            currentPage, response.getStatus(), response.getData() != null);
                    break;
                }

                // For this Facebook API, status = 1 appears to be success, not 200
                if (response.getStatus() != 1) {
                    log.warn("API returned non-success status - Page: {}, Status: {}, Message: '{}'",
                            currentPage, response.getStatus(), response.getMessage());
                    break;
                }

                List<FacebookOrderDto> pageOrders = response.getData().getOrders();
                if (pageOrders == null || pageOrders.isEmpty()) {
                    log.info("No more orders available - stopping at page {}", currentPage);
                    break;
                }

                // Add to buffer (convert to Object for generic processing)
                orderBuffer.addAll(pageOrders);
                totalOrders += pageOrders.size();

                log.debug("Buffered {} orders from page {} (total buffered: {})",
                        pageOrders.size(), currentPage, orderBuffer.size());

                // Flush buffer when full
                if (orderBuffer.size() >= bufferSize) {
                    flushBuffer(orderBuffer, summary);
                    orderBuffer.clear();
                }

                // Check if we got fewer orders than requested (last page)
                if (pageOrders.size() < pageSize) {
                    hasMoreData = false;
                }

                currentPage++;
            }

            // Flush remaining orders in buffer
            if (!orderBuffer.isEmpty()) {
                flushBuffer(orderBuffer, summary);
            }

            // Update summary - ensure non-null values
            summary.setTotalApiCalls(apiCalls);
            if (summary.getPlatformCounts() == null) {
                summary.setPlatformCounts(new HashMap<>());
            }
            summary.addPlatformCount("FACEBOOK", totalOrders);
            summary.markCompleted();

            log.info("✅ Buffered collection completed - Orders: {}, API Calls: {}, Buffer Flushes: {}",
                    totalOrders, apiCalls, (totalOrders > 0 ? (totalOrders + bufferSize - 1) / bufferSize : 0));

        } catch (Exception e) {
            log.error("❌ Buffered collection failed: {}", e.getMessage(), e);
            summary.markFailed(e.getMessage());
        }

        log.info("🔧 DEBUG: Returning summary: {}", summary != null ? "NOT NULL" : "NULL");
        return summary; // Always return summary, never null
    }

    /**
     * Legacy method for backward compatibility (without date parameter)
     */
    public ImportSummary collectWithBuffer(int bufferSize, int pageSize) {
        log.warn("Using legacy collectWithBuffer without date - will use API default");
        return collectWithBuffer("", bufferSize, pageSize);
    }

    /**
     * Flush buffer to database using batch processor
     */
    private void flushBuffer(List<Object> orderBuffer, ImportSummary summary) {
        if (orderBuffer.isEmpty()) return;

        log.debug("🔄 Flushing {} orders to database", orderBuffer.size());

        try {
            // Process orders through batch processor
            var result = batchProcessor.processFacebookOrders(orderBuffer);

            // Track successful processing - simple approach
            int orderCount = orderBuffer.size();

            // Add basic counts to summary (simple estimation for now)
            summary.addTableInsertCount("customers", orderCount);
            summary.addTableInsertCount("orders", orderCount);
            summary.addTableInsertCount("order_items", orderCount * 2);
            summary.addTableInsertCount("products", orderCount);
            summary.addTableInsertCount("geography_info", orderCount);
            summary.addTableInsertCount("payment_info", orderCount);
            summary.addTableInsertCount("shipping_info", orderCount);
            summary.addTableInsertCount("processing_date_info", 1);
            summary.addTableInsertCount("order_status", orderCount);
            summary.addTableInsertCount("order_status_detail", orderCount);
            summary.addTableInsertCount("status", 0);

            // Track DB operations (11 tables per flush)
            summary.setTotalDbOperations(summary.getTotalDbOperations() + 11);

        } catch (Exception e) {
            log.error("Failed to flush buffer: {}", e.getMessage(), e);
            throw e;
        }
    }
}