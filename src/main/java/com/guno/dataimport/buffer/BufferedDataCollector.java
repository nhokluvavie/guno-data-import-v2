package com.guno.dataimport.buffer;

import com.guno.dataimport.api.client.FacebookApiClient;
import com.guno.dataimport.api.client.ShopeeApiClient;
import com.guno.dataimport.api.client.TikTokApiClient;
import com.guno.dataimport.config.PlatformConfig;
import com.guno.dataimport.dto.internal.CollectedData;
import com.guno.dataimport.dto.internal.ImportSummary;
import com.guno.dataimport.dto.platform.facebook.FacebookApiResponse;
import com.guno.dataimport.dto.platform.facebook.FacebookOrderDto;
import com.guno.dataimport.dto.platform.shopee.ShopeeApiResponse;
import com.guno.dataimport.dto.platform.shopee.ShopeeOrderDto;
import com.guno.dataimport.dto.platform.tiktok.TikTokApiResponse;
import com.guno.dataimport.dto.platform.tiktok.TikTokOrderDto;
import com.guno.dataimport.processor.BatchProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class BufferedDataCollector {

    private final FacebookApiClient facebookApiClient;
    private final TikTokApiClient tikTokApiClient;
    private final ShopeeApiClient shopeeApiClient;
    private final BatchProcessor batchProcessor;
    private final PlatformConfig platformConfig;

    // ================================
    // FACEBOOK BUFFERED COLLECTION
    // ================================

    public ImportSummary collectFacebookWithBuffer(String date, int bufferSize, int pageSize) {
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

                if (orders.isEmpty()) {
                    log.info("✅ No more Facebook orders at page {}", currentPage);
                    break;
                }

                orderBuffer.addAll(orders);
                totalOrders += orders.size();
                log.info("📦 Facebook Page {} collected: {} orders (Buffer: {}/{})",
                        currentPage, orders.size(), orderBuffer.size(), bufferSize);

                // Process buffer when full
                if (orderBuffer.size() >= bufferSize) {
                    processBuffer(orderBuffer, "FACEBOOK", summary);
                    orderBuffer.clear();
                }

                // Check if last page
                if (orders.size() < pageSize) {
                    log.info("✅ Facebook last page reached");
                    hasMoreData = false;
                }

                currentPage++;
            }

            // Process remaining orders in buffer
            if (!orderBuffer.isEmpty()) {
                log.info("📦 Processing remaining {} Facebook orders", orderBuffer.size());
                processBuffer(orderBuffer, "FACEBOOK", summary);
            }

            log.info("✅ Facebook collection completed - Total: {}, API Calls: {}", totalOrders, apiCalls);
            return summary;

        } catch (Exception e) {
            log.error("❌ Facebook buffered collection error", e);
            summary.setErrorMessage("Facebook collection failed: " + e.getMessage());
            summary.setStatus("FAILED");
            return summary;
        }
    }

    // ================================
    // TIKTOK BUFFERED COLLECTION
    // ================================

    public ImportSummary collectTikTokWithBuffer(String date, int bufferSize, int pageSize) {
        log.info("🎵 Starting TikTok buffered collection - Date: '{}', Buffer: {}, PageSize: {}",
                date, bufferSize, pageSize);

        ImportSummary summary = ImportSummary.createWithDefaultTables();
        List<Object> orderBuffer = new ArrayList<>(bufferSize);

        int currentPage = 1;
        int totalOrders = 0;
        int filteredOrders = 0;
        int apiCalls = 0;
        boolean hasMoreData = true;

        try {
            while (hasMoreData) {
                log.debug("🔄 TikTok API - Page: {}, Date: '{}'", currentPage, date);

                TikTokApiResponse response = tikTokApiClient.fetchOrders(date, currentPage, pageSize);
                apiCalls++;

                if (response == null || !response.isSuccess() || !response.hasOrders()) {
                    log.warn("TikTok API unsuccessful - Page: {}", currentPage);
                    break;
                }

                List<TikTokOrderDto> orders = response.getOrders();

                // ✅ Filter null tiktok_data
                int beforeFilter = orders.size();
                orders = orders.stream()
                        .filter(order -> order.hasTikTokData())
                        .toList();
                int filtered = beforeFilter - orders.size();
                filteredOrders += filtered;

                if (filtered > 0) {
                    log.debug("   Filtered {} orders with null tiktok_data (page {})", filtered, currentPage);
                }

                if (orders.isEmpty()) {
                    log.info("No valid orders after filtering - stopping");
                    break;
                }

                // Cast to Object for buffer
                orderBuffer.addAll(orders);
                totalOrders += orders.size();

                log.info("📦 TikTok Page {} collected: {} orders (Valid: {}, Filtered: {}, Buffer: {}/{})",
                        currentPage, beforeFilter, orders.size(), filtered, orderBuffer.size(), bufferSize);

                // Process buffer when full
                if (orderBuffer.size() >= bufferSize) {
                    log.info("🔄 Processing buffer: {} TikTok orders", orderBuffer.size());
                    processBuffer(orderBuffer, "TIKTOK", summary);
                    orderBuffer.clear();
                }

                // ✅ FIXED: Check pagination properly
                // Option 1: Use API response hasNextPage (if available)
                Boolean hasNext = response.hasNextPage();
                if (hasNext != null && !hasNext) {
                    log.info("✅ TikTok API indicates no more pages (hasNextPage=false)");
                    hasMoreData = false;
                }
                // Option 2: Fallback to checking if less than pageSize (BEFORE filter)
                else if (beforeFilter < pageSize) {
                    log.info("✅ TikTok last page reached (partial page: {} < {})", beforeFilter, pageSize);
                    hasMoreData = false;
                }
                // ✅ CRITICAL: If full page received, continue to next page
                else {
                    log.debug("   ➡️ Full page received, continuing to next page...");
                    currentPage++;
                }
            }

            // Process remaining orders in buffer
            if (!orderBuffer.isEmpty()) {
                log.info("📦 Processing remaining {} TikTok orders", orderBuffer.size());
                processBuffer(orderBuffer, "TIKTOK", summary);
            }

            summary.setTotalApiCalls(apiCalls);
            log.info("✅ TikTok collection completed - Valid: {}, Filtered: {}, Total API Calls: {}",
                    totalOrders, filteredOrders, apiCalls);

            return summary;

        } catch (Exception e) {
            log.error("❌ TikTok buffered collection error", e);
            summary.setErrorMessage("TikTok collection failed: " + e.getMessage());
            summary.setStatus("FAILED");
            return summary;
        }
    }

    // ================================
    // SHOPEE BUFFERED COLLECTION
    // ================================

    public ImportSummary collectShopeeWithBuffer(String date, int bufferSize, int pageSize) {
        log.info("🚀 Starting Shopee buffered collection - Date: '{}', Buffer: {}, PageSize: {}",
                date, bufferSize, pageSize);

        ImportSummary summary = ImportSummary.createWithDefaultTables();
        List<Object> orderBuffer = new ArrayList<>(bufferSize);
        int currentPage = 1;
        int totalOrders = 0;
        int apiCalls = 0;
        int filteredOrders = 0;
        boolean hasMoreData = true;

        try {
            while (hasMoreData) {
                log.debug("🔄 Calling Shopee API - Page: {}, Date: '{}'", currentPage, date);

                // ✅ CLEAN: Use ShopeeApiResponse
                ShopeeApiResponse response = shopeeApiClient.fetchOrders(date, currentPage, pageSize);
                apiCalls++;

                if (response == null || response.getCode() != 200 ||
                        response.getData() == null || response.getData().getOrders() == null) {
                    log.warn("Shopee API unsuccessful - Page: {}", currentPage);
                    break;
                }

                if (response.getData().getOrders().isEmpty()) {
                    log.info("✅ No more Shopee orders at page {}", currentPage);
                    break;
                }

                // ✅ CLEAN: Direct access to ShopeeOrderDto list
                List<ShopeeOrderDto> validOrders = response.getOrders().stream()
                        .filter(ShopeeOrderDto::hasShopeeData)
                        .toList();

                filteredOrders += (response.getOrderCount() - validOrders.size());

                orderBuffer.addAll(validOrders);
                totalOrders += validOrders.size();

                log.debug("📦 Collected {} Shopee orders - Buffer: {}/{}",
                        validOrders.size(), orderBuffer.size(), bufferSize);

                // Process buffer if full
                if (orderBuffer.size() >= bufferSize) {
                    log.info("💾 Buffer full ({}) - Processing batch", orderBuffer.size());
                    processBuffer(orderBuffer, "SHOPEE", summary);
                    orderBuffer.clear();
                }

                // Check pagination
                if (response.getOrderCount() < pageSize) {
                    log.info("✅ Last page detected");
                    hasMoreData = false;
                } else {
                    currentPage++;
                }
            }

            // Process remaining orders in buffer
            if (!orderBuffer.isEmpty()) {
                log.info("📦 Processing remaining {} Shopee orders", orderBuffer.size());
                processBuffer(orderBuffer, "SHOPEE", summary);
            }

            summary.setTotalApiCalls(apiCalls);
            log.info("✅ Shopee collection completed - Total: {}, Filtered: {}, API Calls: {}",
                    totalOrders, filteredOrders, apiCalls);

            return summary;

        } catch (Exception e) {
            log.error("❌ Shopee buffered collection error", e);
            summary.setErrorMessage("Shopee collection failed: " + e.getMessage());
            summary.setStatus("FAILED");
            return summary;
        }
    }

    // ================================
    // UNIFIED COLLECTION METHOD
    // ================================

    /**
     * Unified collection with same page size for all platforms
     */
    public ImportSummary collectWithBuffer(String date, int bufferSize, int pageSize) {
        log.info("🚀 Starting unified buffered collection for all platforms");

        ImportSummary globalSummary = ImportSummary.createWithDefaultTables();

        // Collect Facebook
        if (platformConfig.isFacebookEnabled()) {
            ImportSummary fbSummary = collectFacebookWithBuffer(date, bufferSize, pageSize);
            mergeSummaries(globalSummary, fbSummary);
        }

        // Collect TikTok
        if (platformConfig.isTikTokEnabled()) {
            ImportSummary ttSummary = collectTikTokWithBuffer(date, bufferSize, pageSize);
            mergeSummaries(globalSummary, ttSummary);
        }

        // Collect Shopee
        if (platformConfig.isShopeeEnabled()) {
            ImportSummary spSummary = collectShopeeWithBuffer(date, bufferSize, pageSize);
            mergeSummaries(globalSummary, spSummary);
        }

        return globalSummary;
    }

    /**
     * Alias for collectWithBuffer() - for compatibility with ApiOrchestrator
     */
    public ImportSummary collectMultiPlatformWithBuffer(String date, int bufferSize, int pageSize) {
        return collectWithBuffer(date, bufferSize, pageSize);
    }

    /**
     * Legacy method - for compatibility
     * Returns CollectedData instead of ImportSummary (not recommended)
     */
    public CollectedData collectMultiPlatformData(int bufferSize) {
        log.warn("collectMultiPlatformData() is deprecated - use collectWithBuffer() instead");
        // Return empty CollectedData for now - this method should not be used
        return new CollectedData();
    }

    /**
     * Dynamic multi-platform collection with different page sizes per platform
     * Used by ApiOrchestrator.collectAndProcessInBatches()
     */
    public ImportSummary collectMultiPlatformWithBufferDynamic(
            String date,
            int bufferSize,
            int facebookPageSize,
            int tiktokPageSize,
            PlatformConfig config) {

        log.info("🚀 Starting DYNAMIC multi-platform buffered collection");
        log.info("   Date: '{}', Buffer: {}", date, bufferSize);
        log.info("   Facebook PageSize: {}, TikTok PageSize: {}", facebookPageSize, tiktokPageSize);

        ImportSummary globalSummary = ImportSummary.createWithDefaultTables();
        globalSummary.setStartTime(LocalDateTime.now());

        try {
            // Collect Facebook with custom page size
            if (config.isFacebookEnabled()) {
                log.info("📘 Collecting Facebook platform data...");
                ImportSummary fbSummary = collectFacebookWithBuffer(date, bufferSize, facebookPageSize);
                mergeSummaries(globalSummary, fbSummary);
            } else {
                log.info("⏭️ Facebook collection DISABLED - skipped");
            }

            // Collect TikTok with custom page size
            if (config.isTikTokEnabled()) {
                log.info("📗 Collecting TikTok platform data...");
                ImportSummary ttSummary = collectTikTokWithBuffer(date, bufferSize, tiktokPageSize);
                mergeSummaries(globalSummary, ttSummary);
            } else {
                log.info("⏭️ TikTok collection DISABLED - skipped");
            }

            // Collect Shopee with default page size (using facebook page size)
            if (config.isShopeeEnabled()) {
                log.info("📙 Collecting Shopee platform data...");
                ImportSummary spSummary = collectShopeeWithBuffer(date, bufferSize, facebookPageSize);
                mergeSummaries(globalSummary, spSummary);
            } else {
                log.info("⏭️ Shopee collection DISABLED - skipped");
            }

            globalSummary.setEndTime(LocalDateTime.now());
            globalSummary.setStatus("COMPLETED");

            log.info("✅ Multi-platform dynamic collection completed");
            log.info("   Facebook: {}, TikTok: {}, Shopee: {}",
                    globalSummary.getPlatformCounts().getOrDefault("FACEBOOK", 0),
                    globalSummary.getPlatformCounts().getOrDefault("TIKTOK", 0),
                    globalSummary.getPlatformCounts().getOrDefault("SHOPEE", 0));

        } catch (Exception e) {
            log.error("❌ Multi-platform dynamic collection failed", e);
            globalSummary.setErrorMessage("Multi-platform collection failed: " + e.getMessage());
            globalSummary.setStatus("FAILED");
        }

        return globalSummary;
    }

    // ================================
    // HELPER METHODS
    // ================================

    private void processBuffer(List<Object> buffer, String platform, ImportSummary summary) {
        try {
            log.info("🔄 Processing buffer for {} - {} orders", platform, buffer.size());

            CollectedData collectedData = new CollectedData();

            switch (platform) {
                case "FACEBOOK":
                    collectedData.setFacebookOrders(new ArrayList<>(buffer));
                    break;
                case "TIKTOK":
                    collectedData.setTikTokOrders(new ArrayList<>(buffer));
                    break;
                case "SHOPEE":
                    collectedData.setShopeeOrders(new ArrayList<>(buffer));
                    break;
            }

            batchProcessor.processCollectedData(collectedData);

        } catch (Exception e) {
            log.error("❌ Buffer processing error for {}", platform, e);
            String currentError = summary.getErrorMessage();
            String newError = platform + " buffer processing failed: " + e.getMessage();
            summary.setErrorMessage(currentError != null ? currentError + "; " + newError : newError);
        }
    }

    private void mergeSummaries(ImportSummary target, ImportSummary source) {
        // Merge statistics from source into target
        if (source == null) return;

        // Merge counts
        target.setTotalApiCalls(target.getTotalApiCalls() + source.getTotalApiCalls());
        target.setTotalDbOperations(target.getTotalDbOperations() + source.getTotalDbOperations());

        // Merge platform counts
        if (source.getPlatformCounts() != null) {
            source.getPlatformCounts().forEach((platform, count) -> {
                target.getPlatformCounts().merge(platform, count, Integer::sum);
            });
        }

        // Merge table insert counts
        if (source.getTableInsertCounts() != null) {
            source.getTableInsertCounts().forEach((table, count) -> {
                target.getTableInsertCounts().merge(table, count, Integer::sum);
            });
        }

        // Merge error messages
        if (source.getErrorMessage() != null && !source.getErrorMessage().isEmpty()) {
            String currentError = target.getErrorMessage();
            target.setErrorMessage(currentError != null ?
                    currentError + "; " + source.getErrorMessage() : source.getErrorMessage());
        }
    }
}