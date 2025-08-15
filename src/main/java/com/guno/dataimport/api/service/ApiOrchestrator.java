package com.guno.dataimport.api.service;

import com.guno.dataimport.api.client.FacebookApiClient;
import com.guno.dataimport.dto.internal.CollectedData;
import com.guno.dataimport.dto.platform.facebook.FacebookApiResponse;
import com.guno.dataimport.processor.BatchProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * API Orchestrator - Coordinates data collection from Facebook platform
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ApiOrchestrator {

    private final FacebookApiClient facebookApiClient;
    private final BatchProcessor batchProcessor;

    private final ExecutorService executorService = Executors.newFixedThreadPool(3);

    /**
     * Collect data from Facebook platform (async)
     */
    public CompletableFuture<CollectedData> collectDataAsync() {
        log.info("Starting async data collection from Facebook");

        return CompletableFuture.supplyAsync(() -> {
            CollectedData collectedData = new CollectedData();

            try {
                // Collect Facebook data
                FacebookApiResponse facebookResponse = facebookApiClient.fetchOrders();
                if (facebookResponse.isSuccess() && facebookResponse.getData() != null) {
                    collectedData.setFacebookOrders(
                            facebookResponse.getData().getOrders().stream()
                                    .map(order -> (Object) order)
                                    .toList()
                    );
                    log.info("Collected {} Facebook orders", facebookResponse.getOrderCount());
                } else {
                    log.warn("Failed to collect Facebook data or empty response");
                }

            } catch (Exception e) {
                log.error("Error collecting Facebook data: {}", e.getMessage(), e);
            }

            log.info("Data collection completed. Total orders: {}", collectedData.getTotalOrders());
            return collectedData;

        }, executorService);
    }

    /**
     * Collect data synchronously
     */
    public CollectedData collectData() {
        log.info("Starting synchronous data collection");

        CollectedData collectedData = new CollectedData();

        try {
            FacebookApiResponse facebookResponse = facebookApiClient.fetchOrders();
            if (facebookResponse.isSuccess() && facebookResponse.getData() != null) {
                collectedData.setFacebookOrders(
                        facebookResponse.getData().getOrders().stream()
                                .map(order -> (Object) order)
                                .toList()
                );
                log.info("Collected {} Facebook orders", facebookResponse.getOrderCount());
            }
        } catch (Exception e) {
            log.error("Error collecting data: {}", e.getMessage(), e);
        }

        log.info("Data collection completed. Total orders: {}", collectedData.getTotalOrders());
        return collectedData;
    }

    /**
     * Check if all APIs are available
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
}