package com.guno.dataimport.api.service;

import com.guno.dataimport.api.client.FacebookApiClient;
import com.guno.dataimport.api.client.ShopeeApiClient;
import com.guno.dataimport.api.client.TikTokApiClient;
import com.guno.dataimport.buffer.BufferedDataCollector;
import com.guno.dataimport.config.PlatformConfig;
import com.guno.dataimport.dto.internal.CollectedData;
import com.guno.dataimport.dto.internal.ImportSummary;
import com.guno.dataimport.dto.platform.facebook.FacebookApiResponse;
import com.guno.dataimport.processor.BatchProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * ApiOrchestrator - PHASE 2: Platform enable/disable support
 *
 * UPDATES:
 * 1. ✅ Inject PlatformConfig
 * 2. ✅ Check platform enabled before collecting
 * 3. ✅ Fix collectAndProcessInBatches() method
 * 4. ✅ Log enabled platforms on startup
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ApiOrchestrator {

    private final FacebookApiClient facebookApiClient;
    private final TikTokApiClient tikTokApiClient;
    private final ShopeeApiClient shopeeApiClient;
    private final BatchProcessor batchProcessor;
    private final BufferedDataCollector bufferedDataCollector;
    private final PlatformConfig platformConfig;
    private static final ZoneId VIETNAM_ZONE = ZoneId.of("Asia/Ho_Chi_Minh");

    private final ExecutorService executorService = Executors.newFixedThreadPool(3);

    // ================================
    // PUBLIC API
    // ================================

    public CollectedData collectData(boolean useBuffer, int bufferSize) {
        if (useBuffer) {
            return collectDataWithBuffer(bufferSize);
        } else {
            return collectSinglePage();
        }
    }

    public CollectedData collectData() {
        return collectData(false, 0);
    }

    /**
     * MAIN METHOD: Process with batches and buffer optimization
     */
    public ImportSummary processInBatches(int pageSize, boolean useBuffer, int bufferSize, String date) {
        logPlatformStatus();

        if (!platformConfig.hasAnyPlatformEnabled()) {
            log.warn("⚠️ No platforms enabled - skipping import");
            return ImportSummary.builder()
                    .totalApiCalls(0)
                    .totalDbOperations(0)
                    .build();
        }

        if (useBuffer) {
            log.info("Using BUFFERED multi-platform processing - Date: {}, Buffer: {}, PageSize: {}",
                    date, bufferSize, pageSize);
            return bufferedDataCollector.collectMultiPlatformWithBuffer(date, bufferSize, pageSize);
        } else {
            log.info("Using STANDARD multi-platform processing - PageSize: {}", pageSize);
            return processPageByPage(pageSize);
        }
    }

    /**
     * FIXED: Default method for scheduler
     */
    public ImportSummary collectAndProcessInBatches() {
        log.info("🚀 Starting scheduled batch import");
        logPlatformStatus();

        // Get current date in GMT+7
        String currentDate = LocalDate.now(VIETNAM_ZONE)
                .toString();

        log.info("📅 Collecting data for date: {} (GMT+7)", currentDate);
        return processInBatches(100, true, 500, currentDate);
    }

    public ImportSummary processPageByPage(int pageSize) {
        log.info("Processing multi-platform data page by page - PageSize: {}", pageSize);
        logPlatformStatus();

        CollectedData data = collectSinglePage();
        if (data.getTotalOrders() == 0) {
            log.warn("No orders collected from any platform");
            return ImportSummary.builder()
                    .totalApiCalls(platformConfig.getEnabledCount())
                    .totalDbOperations(0)
                    .build();
        }

        var result = batchProcessor.processCollectedData(data);

        return ImportSummary.builder()
                .totalApiCalls(platformConfig.getEnabledCount())
                .totalDbOperations(result.getSuccessCount() * 11)
                .processingTimeMs(result.getProcessingTimeMs())
                .platformCounts(data.getPlatformCounts())
                .build();
    }

    public void collectAndProcessInBatchesLegacy() {
        processPageByPage(100);
    }

    // ================================
    // AVAILABILITY CHECKS
    // ================================

    public boolean areApisAvailable() {
        logPlatformStatus();

        boolean anyAvailable = false;

        if (platformConfig.isFacebookEnabled()) {
            boolean available = facebookApiClient.isApiAvailable();
            log.info("Facebook API - Enabled: true, Available: {}", available);
            anyAvailable = anyAvailable || available;
        } else {
            log.info("Facebook API - Enabled: false (skipped)");
        }

        if (platformConfig.isTikTokEnabled()) {
            boolean available = tikTokApiClient.isApiAvailable();
            log.info("TikTok API - Enabled: true, Available: {}", available);
            anyAvailable = anyAvailable || available;
        } else {
            log.info("TikTok API - Enabled: false (skipped)");
        }

        if (platformConfig.isShopeeEnabled()) {
            boolean available = shopeeApiClient.isApiAvailable();
            log.info("Shopee API - Enabled: true, Available: {}", available);
            anyAvailable = anyAvailable || available;
        } else {
            log.info("Shopee API - Enabled: false (skipped)");
        }

        return anyAvailable;
    }

    public boolean isFacebookApiAvailable() {
        return platformConfig.isFacebookEnabled() && facebookApiClient.isApiAvailable();
    }

    public boolean isTikTokApiAvailable() {
        return platformConfig.isTikTokEnabled() && tikTokApiClient.isApiAvailable();
    }

    public boolean isShopeeApiAvailable() {
        return platformConfig.isShopeeEnabled() && shopeeApiClient.isApiAvailable();
    }

    public void shutdown() {
        executorService.shutdown();
    }

    // ================================
    // PRIVATE METHODS
    // ================================

    /**
     * Collect single page from enabled platforms only
     */
    private CollectedData collectSinglePage() {
        log.info("Collecting single page from enabled platforms");
        logPlatformStatus();

        CollectedData data = new CollectedData();

        try {
            // Facebook
            if (platformConfig.isFacebookEnabled()) {
                log.info("📘 Collecting Facebook orders...");
                FacebookApiResponse facebookResponse = facebookApiClient.fetchOrders("", 1, 100);
                if (facebookResponse.getData() != null && facebookResponse.getData().getOrders() != null) {
                    data.setFacebookOrders(facebookResponse.getData().getOrders().stream()
                            .map(order -> (Object) order)
                            .toList());
                    log.info("✅ Collected {} Facebook orders", data.getFacebookOrders().size());
                } else {
                    log.warn("⚠️ Facebook API returned no data");
                }
            } else {
                log.info("⏭️ Facebook collection skipped (disabled)");
            }

            // TikTok
            if (platformConfig.isTikTokEnabled()) {
                log.info("📗 Collecting TikTok orders...");
                FacebookApiResponse tikTokResponse = tikTokApiClient.fetchOrders("", 1, 100);
                if (tikTokResponse.getData() != null && tikTokResponse.getData().getOrders() != null) {
                    data.setTikTokOrders(tikTokResponse.getData().getOrders().stream()
                            .map(order -> (Object) order)
                            .toList());
                    log.info("✅ Collected {} TikTok orders", data.getTikTokOrders().size());
                } else {
                    log.warn("⚠️ TikTok API returned no data");
                }
            } else {
                log.info("⏭️ TikTok collection skipped (disabled)");
            }

            // Shopee
            if (platformConfig.isShopeeEnabled()) {
                log.info("📙 Collecting Shopee orders...");
                FacebookApiResponse shopeeResponse = shopeeApiClient.fetchOrders("", 1, 100);
                if (shopeeResponse.getData() != null && shopeeResponse.getData().getOrders() != null) {
                    data.setShopeeOrders(shopeeResponse.getData().getOrders().stream()
                            .map(order -> (Object) order)
                            .toList());
                    log.info("✅ Collected {} Shopee orders", data.getShopeeOrders().size());
                } else {
                    log.warn("⚠️ Shopee API returned no data");
                }
            } else {
                log.info("⏭️ Shopee collection skipped (disabled)");
            }

            log.info("📊 Total collection: Facebook={}, TikTok={}, Shopee={}, Total={}",
                    data.getFacebookOrders().size(),
                    data.getTikTokOrders().size(),
                    data.getShopeeOrders().size(),
                    data.getTotalOrders());

        } catch (Exception e) {
            log.error("❌ Multi-platform collection error: {}", e.getMessage(), e);
        }

        return data;
    }

    private CollectedData collectDataWithBuffer(int bufferSize) {
        log.info("Collecting multi-platform data with buffer optimization - BufferSize: {}", bufferSize);
        logPlatformStatus();
        return bufferedDataCollector.collectMultiPlatformData(bufferSize);
    }

    /**
     * Log platform configuration status
     */
    private void logPlatformStatus() {
        log.info("🔧 Platform Configuration: {}", platformConfig.getEnabledPlatforms());
        log.info("   - Facebook: {}", platformConfig.isFacebookEnabled() ? "✅ ENABLED" : "❌ DISABLED");
        log.info("   - TikTok: {}", platformConfig.isTikTokEnabled() ? "✅ ENABLED" : "❌ DISABLED");
        log.info("   - Shopee: {}", platformConfig.isShopeeEnabled() ? "✅ ENABLED" : "❌ DISABLED");
    }
}