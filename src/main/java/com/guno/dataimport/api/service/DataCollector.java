package com.guno.dataimport.api.service;

import com.guno.dataimport.dto.internal.CollectedData;
import com.guno.dataimport.dto.internal.ImportSummary;
import com.guno.dataimport.dto.internal.ProcessingResult;
import com.guno.dataimport.dto.platform.facebook.FacebookApiResponse;
import com.guno.dataimport.processor.BatchProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.time.LocalDateTime;

/**
 * Data Collector - Main entry point for data collection operations
 * Handles pagination and batch processing automatically
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class DataCollector {

    private final ApiOrchestrator apiOrchestrator;
    private final BatchProcessor batchProcessor;

    /**
     * Collect data from Facebook platform and return CollectedData (single page)
     */
    public CollectedData collectData() {
        log.info("Starting data collection process");

        try {
            // Collect data from Facebook (single page)
            return apiOrchestrator.collectData();

        } catch (Exception e) {
            log.error("Error during data collection: {}", e.getMessage(), e);
            return new CollectedData(); // Return empty data on error
        }
    }

    /**
     * Collect ALL data with pagination and process each batch immediately
     * Perfect for production: Call API → Process → Save DB → Next page
     */
    public ImportSummary collectAndProcessAllData() {
        log.info("Starting paginated collection and processing");

        ImportSummary summary = ImportSummary.builder()
                .startTime(LocalDateTime.now())
                .parallelMode(false)
                .build();

        int currentPage = 1;
        int totalProcessed = 0;
        int totalApiCalls = 0;
        boolean hasMoreData = true;

        while (hasMoreData) {
            try {
                log.info("Processing page {}", currentPage);

                // Step 1: Call API for current page
                FacebookApiResponse response = apiOrchestrator.getFacebookApiClient()
                        .fetchOrders("", currentPage, 100);
                totalApiCalls++;

                if (response.getData() != null && !response.getData().getOrders().isEmpty()) {
                    // Step 2: Create batch data
                    CollectedData batchData = new CollectedData();
                    batchData.setFacebookOrders(response.getData().getOrders().stream()
                            .map(order -> (Object) order)
                            .toList());

                    log.info("Page {} collected {} orders", currentPage, batchData.getFacebookOrders().size());

                    // Step 3: Process and save to DB immediately
                    ProcessingResult result = batchProcessor.processCollectedData(batchData);
                    totalProcessed += result.getSuccessCount();

                    log.info("Page {} processed: {} success, {} failed",
                            currentPage, result.getSuccessCount(), result.getFailedCount());

                    // Step 4: Check if has more pages
                    hasMoreData = batchData.getFacebookOrders().size() >= 100; // Full page = likely more data
                    currentPage++;

                    // Small delay between pages
                    Thread.sleep(1000);

                } else {
                    log.info("No more data at page {}", currentPage);
                    hasMoreData = false;
                }

            } catch (Exception e) {
                log.error("Error processing page {}: {}", currentPage, e.getMessage(), e);
                hasMoreData = false;
            }
        }

        // Update summary
        summary.addPlatformCount("FACEBOOK", totalProcessed);
        summary.setTotalApiCalls(totalApiCalls);
        summary.setTotalDbOperations(totalProcessed * 4); // Estimate
        summary.setEndTime(LocalDateTime.now());

        log.info("Pagination completed - Total: {} orders, {} API calls, Duration: {}",
                totalProcessed, totalApiCalls, summary.getDurationFormatted());

        return summary;
    }

    /**
     * Check system readiness
     */
    public boolean isSystemReady() {
        boolean ready = apiOrchestrator.areApisAvailable();
        log.info("System readiness check: {}", ready ? "READY" : "NOT READY");
        return ready;
    }