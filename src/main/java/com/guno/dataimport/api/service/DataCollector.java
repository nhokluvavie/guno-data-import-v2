package com.guno.dataimport.api.service;

import com.guno.dataimport.dto.internal.CollectedData;
import com.guno.dataimport.dto.internal.ImportSummary;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.time.LocalDateTime;

/**
 * Data Collector - Main entry point for data collection operations
 * Coordinates with ApiOrchestrator and provides summary reporting
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class DataCollector {

    private final ApiOrchestrator apiOrchestrator;

    /**
     * Collect data from Facebook platform and return CollectedData
     */
    public CollectedData collectData() {
        log.info("Starting data collection process");

        try {
            // Collect data from Facebook
            return apiOrchestrator.collectData();

        } catch (Exception e) {
            log.error("Error during data collection: {}", e.getMessage(), e);
            return new CollectedData(); // Return empty data on error
        }
    }

    /**
     * Collect data asynchronously
     */
    public ImportSummary collectAllDataAsync() {
        log.info("Starting async data collection process");

        ImportSummary summary = ImportSummary.builder()
                .startTime(LocalDateTime.now())
                .parallelMode(true)
                .build();

        try {
            // Use async collection
            CollectedData collectedData = apiOrchestrator.collectDataAsync().get();

            // Update summary
            summary.addPlatformCount("FACEBOOK", collectedData.getFacebookOrders().size());
            summary.setTotalApiCalls(1);

            log.info("Async data collection completed successfully");

        } catch (Exception e) {
            log.error("Error during async data collection: {}", e.getMessage(), e);
        }

        summary.setEndTime(LocalDateTime.now());
        logSummary(summary);

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

    private void logSummary(ImportSummary summary) {
        log.info("=== DATA COLLECTION SUMMARY ===");
        log.info("Duration: {}", summary.getDurationFormatted());
        log.info("Parallel Mode: {}", summary.getParallelMode());
        log.info("Total API Calls: {}", summary.getTotalApiCalls());
        log.info("Platform Counts: {}", summary.getPlatformCounts());
        log.info("===============================");
    }
}