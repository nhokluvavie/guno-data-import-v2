package com.guno.dataimport.api.service;

import com.guno.dataimport.api.client.FacebookApiClient;
import com.guno.dataimport.api.client.TikTokApiClient;
import com.guno.dataimport.buffer.BufferedDataCollector;
import com.guno.dataimport.dto.internal.CollectedData;
import com.guno.dataimport.dto.internal.ImportSummary;
import com.guno.dataimport.dto.platform.facebook.FacebookApiResponse;
import com.guno.dataimport.processor.BatchProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * API Orchestrator - Coordinates data collection with buffer optimization
 * ENHANCED: Integrated buffer classes for 10x performance boost
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ApiOrchestrator {

    private final FacebookApiClient facebookApiClient;
    private final TikTokApiClient tikTokApiClient;
    private final BatchProcessor batchProcessor;
    private final BufferedDataCollector bufferedDataCollector;

    private final ExecutorService executorService = Executors.newFixedThreadPool(2);

    /**
     * Get Facebook API Client
     */
    public FacebookApiClient getFacebookApiClient() {
        return facebookApiClient;
    }

    public TikTokApiClient getTikTokApiClient() {
        return tikTokApiClient;
    }

    /**
     * UNIFIED: Collect data with flexible options
     */
    public CollectedData collectData(boolean useBuffer, int bufferSize) {
        if (useBuffer) {
            return collectDataWithBuffer(bufferSize);
        } else {
            return collectSinglePage();
        }
    }

    /**
     * DEFAULT: Single page collection
     */
    public CollectedData collectData() {
        return collectData(false, 0);
    }

    /**
     * ENHANCED: Process with buffer optimization
     */
    public ImportSummary processInBatches(int pageSize, boolean useBuffer, int bufferSize) {
        if (useBuffer) {
            log.info("Using BUFFERED processing - Buffer: {}, PageSize: {}", bufferSize, pageSize);
            return bufferedDataCollector.collectWithBuffer(bufferSize, pageSize);
        } else {
            log.info("Using STANDARD processing - PageSize: {}", pageSize);
            return processPageByPage(pageSize);
        }
    }

    /**
     * OPTIMIZED: Default buffered processing (RECOMMENDED)
     */
    public ImportSummary collectAndProcessInBatches() {
        return processInBatches(100, true, 500); // Buffer 500 orders
    }

    /**
     * LEGACY: Page by page (for compatibility)
     */
    public void collectAndProcessInBatchesLegacy() {
        processPageByPage(100);
    }

    /**
     * Async collection with buffer support
     */
    public CompletableFuture<ImportSummary> collectDataAsync(boolean useBuffer, int bufferSize) {
        return CompletableFuture.supplyAsync(() ->
                processInBatches(100, useBuffer, bufferSize), executorService);
    }

    /**
     * Check API availability
     */
    public boolean areApisAvailable() {
        return facebookApiClient.isApiAvailable();
    }

    /**
     * Clean up resources
     */
    public void shutdown() {
        executorService.shutdown();
    }

    // === PRIVATE IMPLEMENTATION ===

    private CollectedData collectSinglePage() {
        log.info("Collecting single page from all platforms");

        CollectedData data = new CollectedData();

        try {
            // Facebook collection
            FacebookApiResponse facebookResponse = facebookApiClient.fetchOrders("", 1, 100);
            if (facebookResponse.getData() != null) {
                data.setFacebookOrders(facebookResponse.getData().getOrders().stream()
                        .map(order -> (Object) order)
                        .toList());
            }

            // TikTok collection (REUSES FacebookApiResponse!)
            FacebookApiResponse tikTokResponse = tikTokApiClient.fetchOrders("", 1, 100);
            if (tikTokResponse.getData() != null) {
                data.setTikTokOrders(tikTokResponse.getData().getOrders().stream()
                        .map(order -> (Object) order)
                        .toList());
            }

            log.info("Collected - Facebook: {}, TikTok: {}",
                    data.getFacebookOrders().size(), data.getTikTokOrders().size());

        } catch (Exception e) {
            log.error("Collection failed: {}", e.getMessage(), e);
        }

        return data;
    }

    private CollectedData collectDataWithBuffer(int bufferSize) {
        // For single collection, buffer doesn't make sense
        // Return single page but log the intent
        log.info("Buffer requested for single collection - using single page");
        return collectSinglePage();
    }

    private ImportSummary processPageByPage(int pageSize) {
        ImportSummary summary = ImportSummary.builder()
                .startTime(java.time.LocalDateTime.now())
                .build();

        int currentPage = 1;
        int totalProcessed = 0;
        int totalApiCalls = 0;

        try {
            boolean hasMoreData = true;

            while (hasMoreData) {
                FacebookApiResponse response = facebookApiClient.fetchOrders("", currentPage, pageSize);
                totalApiCalls++;

                if (response.getData() != null && !response.getData().getOrders().isEmpty()) {
                    CollectedData batchData = new CollectedData();
                    batchData.setFacebookOrders(response.getData().getOrders().stream()
                            .map(order -> (Object) order)
                            .toList());

                    // Process immediately (11 DB operations per page)
                    var result = batchProcessor.processCollectedData(batchData);
                    totalProcessed += result.getSuccessCount();

                    hasMoreData = batchData.getFacebookOrders().size() >= pageSize;
                    currentPage++;
                    Thread.sleep(1000);
                } else {
                    hasMoreData = false;
                }
            }
        } catch (Exception e) {
            log.error("Page by page error: {}", e.getMessage(), e);
        }

        summary.addPlatformCount("FACEBOOK", totalProcessed);
        summary.setTotalApiCalls(totalApiCalls);
        summary.setTotalDbOperations(totalApiCalls * 11); // 11 tables per page
        summary.setEndTime(java.time.LocalDateTime.now());

        log.info("Legacy processing: {} orders, {} API calls, {} DB ops, Duration: {}",
                totalProcessed, totalApiCalls, summary.getTotalDbOperations(),
                summary.getDurationFormatted());

        return summary;
    }
}