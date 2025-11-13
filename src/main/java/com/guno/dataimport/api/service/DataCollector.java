package com.guno.dataimport.api.service;

import com.guno.dataimport.dto.internal.CollectedData;
import com.guno.dataimport.dto.internal.ImportSummary;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Data Collector - Unified interface for data collection strategies
 * OPTIMIZED: Clean interface with buffer support
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class DataCollector {

    private final ApiOrchestrator apiOrchestrator;

    /**
     * RECOMMENDED: Buffered collection (10x faster)
     */
    public ImportSummary collectAndProcessAllData() {
        log.info("Starting OPTIMIZED collection with buffering");
        return apiOrchestrator.collectAndProcessInBatches(); // Defaults to buffered
    }

    /**
     * LEGACY: Page by page (for compatibility)
     */
    public ImportSummary collectAndProcessAllDataLegacy() {
        log.info("Starting LEGACY collection (page by page)");
        apiOrchestrator.collectAndProcessInBatchesLegacy();
        return ImportSummary.builder()
                .startTime(java.time.LocalDateTime.now())
                .endTime(java.time.LocalDateTime.now())
                .build();
    }

    /**
     * FLEXIBLE: Custom collection strategy
     */
//    public ImportSummary collectAndProcess(int pageSize, boolean useBuffer, int bufferSize) {
//        log.info("Custom collection - PageSize: {}, Buffer: {}, Size: {}",
//                pageSize, useBuffer, bufferSize);
//        return apiOrchestrator.processInBatches(pageSize, useBuffer, bufferSize);
//    }

    /**
     * Single page collection
     */
    public CollectedData collectData() {
        return apiOrchestrator.collectData();
    }

    /**
     * System readiness check
     */
    public boolean isSystemReady() {
        return apiOrchestrator.areApisAvailable();
    }

    /**
     * REMOVED: processBatches() - redundant with collectAndProcessAllData()
     */
}