package com.guno.dataimport.scheduler;

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
     * Execute import process
     */
    private ImportSummary executeImport() {
        ImportSummary summary = ImportSummary.builder()
                .startTime(LocalDateTime.now())
                .parallelMode(false)
                .build();

        try {
            // Step 1: Check system readiness
            if (!dataCollector.isSystemReady()) {
                log.error("System not ready for import - APIs unavailable");
                return summary;
            }

            // Step 2: Collect data
            log.info("Step 1/4: Collecting data from Facebook API");
            CollectedData collectedData = dataCollector.collectData();

            if (collectedData.isEmpty()) {
                log.warn("No data collected - import skipped");
                summary.addPlatformCount("FACEBOOK", 0);
                return summary;
            }

            summary.addPlatformCount("FACEBOOK", collectedData.getFacebookOrders().size());
            summary.setTotalApiCalls(1);

            // Step 3: Validate data
            log.info("Step 2/4: Validating collected data");
            var validationErrors = validationProcessor.validateCollectedData(collectedData);

            if (!validationProcessor.isDataValid(collectedData)) {
                log.error("Data validation failed: {}",
                        validationProcessor.getValidationSummary(validationErrors));
                return summary;
            }

            log.info("Data validation passed: {}",
                    validationProcessor.getValidationSummary(validationErrors));

            // Step 4: Process and save data
            log.info("Step 3/4: Processing and saving data to database");
            ProcessingResult processingResult = batchProcessor.processCollectedData(collectedData);

            // Step 5: Update summary
            log.info("Step 4/4: Finalizing import summary");
            summary.addEntityCount("CUSTOMERS", processingResult.getSuccessCount());
            summary.addEntityCount("ORDERS", processingResult.getSuccessCount());
            summary.addEntityCount("PRODUCTS", processingResult.getSuccessCount());
            summary.setTotalDbOperations(processingResult.getSuccessCount() * 4); // Estimate

            if (!processingResult.isSuccess()) {
                log.warn("Processing completed with {} errors", processingResult.getFailedCount());
            }

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
        log.info("Entity Counts: {}", summary.getEntityCounts());
        log.info("=========================");
    }
}