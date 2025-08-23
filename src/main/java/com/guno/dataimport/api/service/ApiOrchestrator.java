package com.guno.dataimport.api.service;

import com.guno.dataimport.api.client.FacebookApiClient;
import com.guno.dataimport.api.client.ShopeeApiClient;
import com.guno.dataimport.api.client.TikTokApiClient;
import com.guno.dataimport.buffer.BufferedDataCollector;
import com.guno.dataimport.dto.internal.CollectedData;
import com.guno.dataimport.dto.internal.ImportSummary;
import com.guno.dataimport.dto.platform.facebook.FacebookApiResponse;
import com.guno.dataimport.processor.BatchProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
@RequiredArgsConstructor
@Slf4j
public class ApiOrchestrator {

    private final FacebookApiClient facebookApiClient;
    private final TikTokApiClient tikTokApiClient;
    private final ShopeeApiClient shopeeApiClient;
    private final BatchProcessor batchProcessor;
    private final BufferedDataCollector bufferedDataCollector;

    private final ExecutorService executorService = Executors.newFixedThreadPool(3);

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

    public ImportSummary processInBatches(int pageSize, boolean useBuffer, int bufferSize) {
        if (useBuffer) {
            log.info("Using BUFFERED multi-platform processing - Buffer: {}, PageSize: {}", bufferSize, pageSize);
            return bufferedDataCollector.collectWithBuffer(bufferSize, pageSize);
        } else {
            log.info("Using STANDARD multi-platform processing - PageSize: {}", pageSize);
            return processPageByPage(pageSize);
        }
    }

    public ImportSummary collectAndProcessInBatches() {
        return processInBatches(100, true, 500);
    }

    public ImportSummary processPageByPage(int pageSize) {
        log.info("Processing multi-platform data page by page - PageSize: {}", pageSize);

        CollectedData data = collectSinglePage();
        if (data.getTotalOrders() == 0) {
            log.warn("No orders collected from any platform");
            return ImportSummary.builder()
                    .totalApiCalls(2)
                    .totalDbOperations(0)
                    .build();
        }

        var result = batchProcessor.processCollectedData(data);

        return ImportSummary.builder()
                .totalApiCalls(2)
                .totalDbOperations(result.getSuccessCount() * 11)
                .processingTimeMs(result.getProcessingTimeMs())
                .platformCounts(data.getPlatformCounts())
                .build();
    }

    public void collectAndProcessInBatchesLegacy() {
        processPageByPage(100);
    }

    public boolean areApisAvailable() {
        boolean facebookAvailable = facebookApiClient.isApiAvailable();
        boolean tikTokAvailable = tikTokApiClient.isApiAvailable();
        boolean shopeeAvailable = shopeeApiClient.isApiAvailable();

        log.info("API Availability Check - Facebook: {}, TikTok: {}, Shopee: {}", facebookAvailable, tikTokAvailable, shopeeAvailable);

        return facebookAvailable || tikTokAvailable || shopeeAvailable;
    }

    public boolean isFacebookApiAvailable() {
        return facebookApiClient.isApiAvailable();
    }

    public boolean isTikTokApiAvailable() {
        return tikTokApiClient.isApiAvailable();
    }

    public boolean isShopeeApiAvailable() {
        return shopeeApiClient.isApiAvailable();
    }

    public void shutdown() {
        executorService.shutdown();
    }

    // Private methods
    private CollectedData collectSinglePage() {
        log.info("Collecting single page from all platforms (Facebook + TikTok)");

        CollectedData data = new CollectedData();

        try {
            FacebookApiResponse facebookResponse = facebookApiClient.fetchOrders("", 1, 100);
            if (facebookResponse.getData() != null && facebookResponse.getData().getOrders() != null) {
                data.setFacebookOrders(facebookResponse.getData().getOrders().stream()
                        .map(order -> (Object) order)
                        .toList());
                log.info("Collected {} Facebook orders", data.getFacebookOrders().size());
            }

            FacebookApiResponse tikTokResponse = tikTokApiClient.fetchOrders("", 1, 100);
            if (tikTokResponse.getData() != null && tikTokResponse.getData().getOrders() != null) {
                data.setTikTokOrders(tikTokResponse.getData().getOrders().stream()
                        .map(order -> (Object) order)
                        .toList());
                log.info("Collected {} TikTok orders", data.getTikTokOrders().size());
            }

            FacebookApiResponse shopeeResponse = shopeeApiClient.fetchOrders("", 1, 100);
            if (shopeeResponse.getData() != null && shopeeResponse.getData().getOrders() != null) {
                data.setShopeeOrders(shopeeResponse.getData().getOrders().stream()
                        .map(order -> (Object) order)
                        .toList());
                log.info("Collected {} Shopee orders", data.getShopeeOrders().size());
            }

            log.info("Total multi-platform collection: Facebook={}, TikTok={}, Shopee={}, Total={}",
                    data.getFacebookOrders().size(),
                    data.getTikTokOrders().size(),
                    data.getShopeeOrders().size(),
                    data.getTotalOrders());

        } catch (Exception e) {
            log.error("Multi-platform collection error: {}", e.getMessage(), e);
        }

        return data;
    }

    private CollectedData collectDataWithBuffer(int bufferSize) {
        log.info("Collecting multi-platform data with buffer optimization - BufferSize: {}", bufferSize);
        return bufferedDataCollector.collectMultiPlatformData(bufferSize);
    }

    public String getProcessingStats() {
        return String.format(
                "ApiOrchestrator: Facebook API=%s, TikTok API=%s, Shopee API=%s, Executor=%s",
                isFacebookApiAvailable() ? "Available" : "Unavailable",
                isTikTokApiAvailable() ? "Available" : "Unavailable",
                isShopeeApiAvailable() ? "Available" : "Unavailable",
                executorService.isShutdown() ? "Shutdown" : "Active"
        );
    }

    public boolean isSystemReady() {
        try {
            boolean batchReady = batchProcessor.isSystemReady();
            boolean bufferedReady = bufferedDataCollector != null;
            boolean clientsReady = facebookApiClient != null && tikTokApiClient != null && shopeeApiClient != null;

            return batchReady && bufferedReady && clientsReady;
        } catch (Exception e) {
            log.warn("System readiness check failed: {}", e.getMessage());
            return false;
        }
    }
}