package com.guno.dataimport.buffer;

import com.guno.dataimport.api.client.FacebookApiClient;
import com.guno.dataimport.api.client.TikTokApiClient;
import com.guno.dataimport.config.PlatformConfig;
import com.guno.dataimport.dto.internal.CollectedData;
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

@Service
@RequiredArgsConstructor
@Slf4j
public class BufferedDataCollector {

    private final FacebookApiClient facebookApiClient;
    private final TikTokApiClient tikTokApiClient;
    private final BatchProcessor batchProcessor;
    private final PlatformConfig platformConfig;

    public ImportSummary collectWithBuffer(String date, int bufferSize, int pageSize) {
        log.info("🚀 Starting Facebook buffered collection - Date: '{}', Buffer: {}, PageSize: {}",
                date, bufferSize, pageSize);

        ImportSummary summary = ImportSummary.createWithDefaultTables();
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

                if (response == null || response.getCode() != 200 ||
                        response.getData() == null || response.getData().getOrders() == null) {
                    log.warn("Facebook API unsuccessful - Page: {}", currentPage);
                    break;
                }

                List<FacebookOrderDto> orders = response.getData().getOrders();
                int ordersInPage = orders.size();

                totalOrders += ordersInPage;
                orderBuffer.addAll(orders.stream().map(order -> (Object) order).toList());

                if (orderBuffer.size() >= bufferSize) {
                    flushBuffer(orderBuffer, summary);
                    orderBuffer.clear();
                }

                hasMoreData = ordersInPage >= pageSize;
                currentPage++;

                if (currentPage > 100) {
                    log.warn("Reached maximum page limit (100) for safety");
                    break;
                }
            }

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

    public CollectedData collectData() {
        log.info("Collecting single page data from all platforms (Facebook + TikTok)");

        CollectedData data = new CollectedData();

        try {
            if (platformConfig.isFacebookEnabled()) {
                log.debug("Collecting Facebook data...");
                FacebookApiResponse facebookResponse = facebookApiClient.fetchOrders();

                if (facebookResponse != null && facebookResponse.getData() != null &&
                        facebookResponse.getData().getOrders() != null) {

                    data.setFacebookOrders(facebookResponse.getData().getOrders().stream()
                            .map(order -> (Object) order)
                            .toList());

                    log.info("Collected {} Facebook orders", data.getFacebookOrders().size());
                } else {
                    log.warn("No Facebook orders collected");
                }
            } else {
                log.info("⏭️ Facebook collection skipped (disabled)");
            }

            if (platformConfig.isTikTokEnabled()) {
                log.debug("Collecting TikTok data...");
                FacebookApiResponse tikTokResponse = tikTokApiClient.fetchOrders();

                if (tikTokResponse != null && tikTokResponse.getData() != null &&
                        tikTokResponse.getData().getOrders() != null) {

                    data.setTikTokOrders(tikTokResponse.getData().getOrders().stream()
                            .map(order -> (Object) order)
                            .toList());

                    log.info("Collected {} TikTok orders", data.getTikTokOrders().size());
                } else {
                    log.warn("No TikTok orders collected");
                }
            } else {
                log.info("⏭️ TikTok collection skipped (disabled)");
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

    public CollectedData collectMultiPlatformData(int bufferSize) {
        log.info("Collecting multi-platform data with buffer optimization - BufferSize: {}", bufferSize);
        return collectData();
    }

    public ImportSummary collectMultiPlatformWithBuffer(String date, int bufferSize, int pageSize) {
        log.info("🚀 Starting multi-platform buffered collection - Date: '{}', Buffer: {}, PageSize: {}",
                date, bufferSize, pageSize);

        ImportSummary summary = ImportSummary.createWithDefaultTables();

        try {
            if (platformConfig.isFacebookEnabled()) {
                log.info("📘 Collecting Facebook platform data...");
                ImportSummary facebookSummary = collectPlatformData("FACEBOOK", date, bufferSize, pageSize);
                summary.merge(facebookSummary);
            } else {
                log.info("⏭️ Facebook collection skipped (disabled)");
            }

            if (platformConfig.isTikTokEnabled()) {
                log.info("📗 Collecting TikTok platform data...");
                ImportSummary tikTokSummary = collectPlatformData("TIKTOK", date, bufferSize, pageSize);
                summary.merge(tikTokSummary);
            } else {
                log.info("⏭️ TikTok collection skipped (disabled)");
            }

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

    public ImportSummary collectMultiPlatformWithBufferDynamic(
            String date, int bufferSize, int facebookPageSize, int tiktokPageSize, PlatformConfig config) {

        log.info("🚀 Starting multi-platform buffered collection - Date: '{}', Buffer: {}", date, bufferSize);
        log.info("   Facebook PageSize: {}, TikTok PageSize: {}", facebookPageSize, tiktokPageSize);

        ImportSummary summary = ImportSummary.createWithDefaultTables();
        summary.setStartTime(LocalDateTime.now());

        try {
            if (config.isFacebookEnabled()) {
                log.info("📘 Collecting Facebook platform data...");
                ImportSummary fbSummary = collectPlatformData("FACEBOOK", date, bufferSize, facebookPageSize);
                if (fbSummary != null) {
                    summary.merge(fbSummary);
                }
            } else {
                log.info("⏭️ Facebook collection DISABLED - skipped");
            }

            if (config.isTikTokEnabled()) {
                log.info("📗 Collecting TikTok platform data...");
                ImportSummary ttSummary = collectPlatformData("TIKTOK", date, bufferSize, tiktokPageSize);
                if (ttSummary != null) {
                    summary.merge(ttSummary);
                }
            } else {
                log.info("⏭️ TikTok collection DISABLED - skipped");
            }

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

    private ImportSummary collectPlatformData(String platform, String date, int bufferSize, int pageSize) {
        log.info("Collecting {} platform data...", platform);

        ImportSummary platformSummary = ImportSummary.createWithDefaultTables();
        List<Object> orderBuffer = new ArrayList<>(bufferSize);

        int currentPage = 1;
        int totalOrders = 0;
        int apiCalls = 0;
        boolean hasMoreData = true;

        try {
            while (hasMoreData) {
                FacebookApiResponse response = null;

                if ("FACEBOOK".equals(platform)) {
                    response = facebookApiClient.fetchOrders(date, currentPage, pageSize);
                } else if ("TIKTOK".equals(platform)) {
                    response = tikTokApiClient.fetchOrders(date, currentPage, pageSize);
                }

                apiCalls++;

                if (response == null || response.getCode() != 200 ||
                        response.getData() == null ||
                        response.getData().getOrders() == null) {
                    log.warn("{} API unsuccessful - Page: {}, Code: {}, Data: {}, Orders: {}",
                            platform, currentPage,
                            response != null ? response.getCode() : "null",
                            response != null && response.getData() != null ? "exists" : "null",
                            response != null && response.getData() != null && response.getData().getOrders() != null ? "exists" : "null");
                    break;
                }

                List<FacebookOrderDto> orders = response.getData().getOrders();
                int ordersInPage = orders.size();

                log.info("✅ Received {} {} orders on page {}", ordersInPage, platform, currentPage);

                totalOrders += ordersInPage;
                orderBuffer.addAll(orders.stream().map(order -> (Object) order).toList());

                if (orderBuffer.size() >= bufferSize) {
                    log.info("🔄 Flushing {} {} orders to database", orderBuffer.size(), platform);
                    flushPlatformBuffer(orderBuffer, platform, platformSummary);
                    orderBuffer.clear();
                }

                hasMoreData = ordersInPage >= pageSize;
                currentPage++;

                if (currentPage > 50) {
                    log.warn("Reached platform page limit (50) for {}", platform);
                    break;
                }
            }

            if (!orderBuffer.isEmpty()) {
                log.info("🔄 Flushing remaining {} {} orders", orderBuffer.size(), platform);
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
            summary.addTableInsertCount("order_status", orderCount);
            summary.addTableInsertCount("order_status_detail", orderCount);
            summary.addTableInsertCount("processing_date_info", 1);

            summary.setTotalDbOperations(summary.getTotalDbOperations() + 11);

        } catch (Exception e) {
            log.error("Failed to flush Facebook buffer: {}", e.getMessage(), e);
            throw new RuntimeException("Buffer flush failed", e);
        }
    }

    private void flushPlatformBuffer(List<Object> orderBuffer, String platform, ImportSummary summary) {
        if (orderBuffer.isEmpty()) return;

        try {
            CollectedData data = new CollectedData();

            if ("FACEBOOK".equals(platform)) {
                data.setFacebookOrders(orderBuffer);
                log.debug("Processing {} Facebook orders", orderBuffer.size());
            } else if ("TIKTOK".equals(platform)) {
                data.setTikTokOrders(orderBuffer);
                log.debug("Processing {} TikTok orders", orderBuffer.size());
            }

            var result = batchProcessor.processCollectedData(data);

            int orderCount = orderBuffer.size();
            summary.addTableInsertCount("customers", orderCount);
            summary.addTableInsertCount("orders", orderCount);
            summary.addTableInsertCount("order_items", orderCount * 2);
            summary.addTableInsertCount("products", orderCount);
            summary.addTableInsertCount("geography_info", orderCount);
            summary.addTableInsertCount("payment_info", orderCount);
            summary.addTableInsertCount("shipping_info", orderCount);
            summary.addTableInsertCount("order_status", orderCount);
            summary.addTableInsertCount("order_status_detail", orderCount);
            summary.addTableInsertCount("processing_date_info", 1);

            summary.setTotalDbOperations(summary.getTotalDbOperations() + 11);

            log.debug("✅ Flushed {} {} orders successfully", orderCount, platform);

        } catch (Exception e) {
            log.error("Failed to flush {} buffer: {}", platform, e.getMessage(), e);
            throw new RuntimeException("Buffer flush failed for " + platform, e);
        }
    }
}