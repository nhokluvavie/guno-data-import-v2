// =============================================
// COMPLETE BufferedDataCollector.java with TikTok Support
// =============================================

package com.guno.dataimport.buffer;

import com.guno.dataimport.api.client.FacebookApiClient;
import com.guno.dataimport.api.client.TikTokApiClient;  // ADD THIS
import com.guno.dataimport.dto.internal.CollectedData;  // ADD THIS
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
 * BufferedDataCollector - High-performance multi-platform data collection with buffering
 * ENHANCED: Now supports both Facebook and TikTok platforms
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class BufferedDataCollector {

    private final FacebookApiClient facebookApiClient;
    private final TikTokApiClient tikTokApiClient;  // ADD THIS
    private final BatchProcessor batchProcessor;

    /**
     * EXISTING: Facebook-only buffered collection (backward compatibility)
     */
    public ImportSummary collectWithBuffer(String date, int bufferSize, int pageSize) {
        log.info("🚀 Starting Facebook buffered collection - Date: '{}', Buffer: {}, PageSize: {}",
                date, bufferSize, pageSize);

        ImportSummary summary = ImportSummary.createWithDefaultTables();
        if (summary == null) {
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
                log.debug("🔄 Calling Facebook API - Page: {}, Date: '{}'", currentPage, date);

                FacebookApiResponse response = facebookApiClient.fetchOrders(date, currentPage, pageSize);
                apiCalls++;

                if (response == null) {
                    log.warn("Facebook API call returned null response - Page: {}", currentPage);
                    break;
                }

                log.debug("Facebook API Response - Status: {}, Code: {}, Message: '{}'",
                        response.getStatus(), response.getCode(), response.getMessage());

                if (response.getCode() == 200 && response.getData() != null &&
                        response.getData().getOrders() != null) {

                    List<FacebookOrderDto> orders = response.getData().getOrders();
                    log.debug("Received {} Facebook orders on page {}", orders.size(), currentPage);

                    // Add to buffer
                    orderBuffer.addAll(orders.stream().map(order -> (Object) order).toList());
                    totalOrders += orders.size();

                    // Flush buffer if full
                    if (orderBuffer.size() >= bufferSize) {
                        flushBuffer(orderBuffer, summary);
                        orderBuffer.clear();
                    }

                    // Check if more data available
                    hasMoreData = orders.size() >= pageSize;
                    currentPage++;

                } else {
                    log.warn("Facebook API unsuccessful or no data - Status: {}, Message: '{}'",
                            response.getStatus(), response.getMessage());
                    hasMoreData = false;
                }

                if (currentPage > 100) {
                    log.warn("Reached maximum page limit (100) for safety");
                    break;
                }
            }

            // Flush remaining orders
            if (!orderBuffer.isEmpty()) {
                flushBuffer(orderBuffer, summary);
            }

            summary.setTotalApiCalls(apiCalls);
            summary.addPlatformCount("FACEBOOK", totalOrders);
            summary.setEndTime(LocalDateTime.now());
            summary.setStatus("SUCCESS");

            log.info("✅ Facebook buffered collection completed - {} orders, {} API calls, Duration: {}",
                    totalOrders, apiCalls, summary.getDurationFormatted());

        } catch (Exception e) {
            log.error("❌ Facebook buffered collection failed: {}", e.getMessage(), e);
            summary.markFailed(e.getMessage());
        }

        return summary;
    }

    /**
     * LEGACY: Backward compatibility method
     */
    public ImportSummary collectWithBuffer(int bufferSize, int pageSize) {
        log.warn("Using legacy collectWithBuffer without date - will use API default");
        return collectWithBuffer("", bufferSize, pageSize);
    }

    /**
     * NEW: Single page data collection for all platforms
     */
    public CollectedData collectData() {
        log.info("Collecting single page data from all platforms (Facebook + TikTok)");

        CollectedData data = new CollectedData();

        try {
            // Collect Facebook data
            log.debug("Collecting Facebook data...");
            FacebookApiResponse facebookResponse = facebookApiClient.fetchOrders("", 1, 100);

            if (facebookResponse != null && facebookResponse.getData() != null &&
                    facebookResponse.getData().getOrders() != null) {

                data.setFacebookOrders(facebookResponse.getData().getOrders().stream()
                        .map(order -> (Object) order)
                        .toList());

                log.info("Collected {} Facebook orders", data.getFacebookOrders().size());
            } else {
                log.warn("No Facebook orders collected");
            }

            // Collect TikTok data
            log.debug("Collecting TikTok data...");
            FacebookApiResponse tikTokResponse = tikTokApiClient.fetchOrders("", 1, 100);

            if (tikTokResponse != null && tikTokResponse.getData() != null &&
                    tikTokResponse.getData().getOrders() != null) {

                data.setTikTokOrders(tikTokResponse.getData().getOrders().stream()
                        .map(order -> (Object) order)
                        .toList());

                log.info("Collected {} TikTok orders", data.getTikTokOrders().size());
            } else {
                log.warn("No TikTok orders collected");
            }

            log.info("Total collection: Facebook={}, TikTok={}, Total={}",
                    data.getFacebookOrders().size(),
                    data.getTikTokOrders().size(),
                    data.getTotalOrders());

        } catch (Exception e) {
            log.error("Multi-platform data collection failed: {}", e.getMessage(), e);
        }

        return data;
    }

    /**
     * NEW: Multi-platform buffered collection with enhanced buffer strategy
     */
    public CollectedData collectMultiPlatformData(int bufferSize) {
        log.info("Collecting multi-platform data with buffer optimization - BufferSize: {}", bufferSize);

        // For now, use simple single-page collection
        // Can be enhanced later for true multi-platform buffering
        return collectData();
    }

    /**
     * NEW: Multi-platform buffered collection with full control
     */
    public ImportSummary collectMultiPlatformWithBuffer(String date, int bufferSize, int pageSize) {
        log.info("🚀 Starting multi-platform buffered collection - Date: '{}', Buffer: {}, PageSize: {}",
                date, bufferSize, pageSize);

        ImportSummary summary = ImportSummary.createWithDefaultTables();
        if (summary == null) {
            summary = ImportSummary.builder()
                    .startTime(LocalDateTime.now())
                    .status("FAILED")
                    .errorMessage("Failed to create summary")
                    .platformCounts(new HashMap<>())
                    .tableInsertCounts(new HashMap<>())
                    .build();
        }

        try {
            // Collect Facebook data
            log.info("Collecting Facebook platform data...");
            ImportSummary facebookSummary = collectPlatformData("FACEBOOK", date, bufferSize, pageSize);
            summary.merge(facebookSummary);

            // Collect TikTok data
            log.info("Collecting TikTok platform data...");
            ImportSummary tikTokSummary = collectPlatformData("TIKTOK", date, bufferSize, pageSize);
            summary.merge(tikTokSummary);

            summary.setEndTime(LocalDateTime.now());
            summary.setStatus("SUCCESS");

            log.info("✅ Multi-platform collection completed - Facebook: {}, TikTok: {}, Total Duration: {}",
                    summary.getPlatformCount("FACEBOOK"),
                    summary.getPlatformCount("TIKTOK"),
                    summary.getDurationFormatted());

        } catch (Exception e) {
            log.error("❌ Multi-platform collection failed: {}", e.getMessage(), e);
            summary.markFailed(e.getMessage());
        }

        return summary;
    }

    /**
     * PRIVATE: Collect data from specific platform
     */
    private ImportSummary collectPlatformData(String platform, String date, int bufferSize, int pageSize) {
        log.debug("Collecting {} platform data...", platform);

        ImportSummary platformSummary = ImportSummary.createWithDefaultTables();
        List<Object> orderBuffer = new ArrayList<>(bufferSize);

        int currentPage = 1;
        int totalOrders = 0;
        int apiCalls = 0;
        boolean hasMoreData = true;

        try {
            while (hasMoreData) {
                FacebookApiResponse response = null;

                // Call appropriate API based on platform
                if ("FACEBOOK".equals(platform)) {
                    response = facebookApiClient.fetchOrders(date, currentPage, pageSize);
                } else if ("TIKTOK".equals(platform)) {
                    response = tikTokApiClient.fetchOrders(date, currentPage, pageSize);
                }

                apiCalls++;

                if (response == null) {
                    log.warn("{} API call returned null response - Page: {}", platform, currentPage);
                    break;
                }

                if (response.isSuccess() && response.getData() != null &&
                        response.getData().getOrders() != null) {

                    List<FacebookOrderDto> orders = response.getData().getOrders();
                    log.debug("Received {} {} orders on page {}", orders.size(), platform, currentPage);

                    // Add to buffer
                    orderBuffer.addAll(orders.stream().map(order -> (Object) order).toList());
                    totalOrders += orders.size();

                    // Flush buffer if full
                    if (orderBuffer.size() >= bufferSize) {
                        flushPlatformBuffer(orderBuffer, platform, platformSummary);
                        orderBuffer.clear();
                    }

                    hasMoreData = orders.size() >= pageSize;
                    currentPage++;

                } else {
                    hasMoreData = false;
                }

                if (currentPage > 50) { // Lower limit for multi-platform
                    log.warn("Reached platform page limit (50) for {}", platform);
                    break;
                }
            }

            // Flush remaining orders
            if (!orderBuffer.isEmpty()) {
                flushPlatformBuffer(orderBuffer, platform, platformSummary);
            }

            platformSummary.setTotalApiCalls(apiCalls);
            platformSummary.addPlatformCount(platform, totalOrders);

            log.info("✅ {} collection completed - {} orders, {} API calls",
                    platform, totalOrders, apiCalls);

        } catch (Exception e) {
            log.error("❌ {} collection failed: {}", platform, e.getMessage(), e);
            platformSummary.markFailed(e.getMessage());
        }

        return platformSummary;
    }

    /**
     * PRIVATE: Flush buffer to database using batch processor (Facebook only for now)
     */
    private void flushBuffer(List<Object> orderBuffer, ImportSummary summary) {
        if (orderBuffer.isEmpty()) return;

        log.debug("🔄 Flushing {} Facebook orders to database", orderBuffer.size());

        try {
            var result = batchProcessor.processFacebookOrders(orderBuffer);

            int orderCount = orderBuffer.size();
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

            summary.setTotalDbOperations(summary.getTotalDbOperations() + 11);

        } catch (Exception e) {
            log.error("Failed to flush Facebook buffer: {}", e.getMessage(), e);
            throw e;
        }
    }

    /**
     * PRIVATE: Flush platform-specific buffer to database
     */
    private void flushPlatformBuffer(List<Object> orderBuffer, String platform, ImportSummary summary) {
        if (orderBuffer.isEmpty()) return;

        log.debug("🔄 Flushing {} {} orders to database", orderBuffer.size(), platform);

        try {
            if ("FACEBOOK".equals(platform)) {
                var result = batchProcessor.processFacebookOrders(orderBuffer);
            } else if ("TIKTOK".equals(platform)) {
                var result = batchProcessor.processTikTokOrders(orderBuffer);
            }

            int orderCount = orderBuffer.size();

            // Add table counts (same for all platforms)
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

            summary.setTotalDbOperations(summary.getTotalDbOperations() + 11);

        } catch (Exception e) {
            log.error("Failed to flush {} buffer: {}", platform, e.getMessage(), e);
            throw e;
        }
    }
}