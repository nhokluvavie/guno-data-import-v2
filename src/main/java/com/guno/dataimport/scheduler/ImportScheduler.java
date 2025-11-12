package com.guno.dataimport.scheduler;

import com.guno.dataimport.api.service.ApiOrchestrator;
import com.guno.dataimport.api.service.DataCollector;
import com.guno.dataimport.config.PlatformConfig;
import com.guno.dataimport.dto.internal.ImportSummary;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * ImportScheduler - PHASE 3: Enhanced with Platform Config
 *
 * UPDATES:
 * 1. ✅ Inject PlatformConfig
 * 2. ✅ Log platform status at startup and each run
 * 3. ✅ Handle no platforms enabled gracefully
 * 4. ✅ Better error messages with platform context
 * 5. ✅ Show which platforms are processing
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ImportScheduler {

    private final DataCollector dataCollector;
    private final ApiOrchestrator apiOrchestrator;
    private final PlatformConfig platformConfig;

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // ================================
    // SCHEDULED IMPORT
    // ================================

    @Scheduled(cron = "${scheduler.import.cron:0 */5 * * * *}")
    public void scheduledImport() {
        String startTime = LocalDateTime.now().format(TIME_FORMATTER);
        log.info("╔════════════════════════════════════════════════════════════╗");
        log.info("║   SCHEDULED IMPORT STARTED at {}            ║", startTime);
        log.info("╚════════════════════════════════════════════════════════════╝");

        logPlatformConfiguration();

        try {
            ImportSummary summary = executeImport();
            logImportSummary(summary, "SCHEDULED");
        } catch (Exception e) {
            log.error("❌ Scheduled import failed: {}", e.getMessage(), e);
            logErrorContext(e);
        }

        String endTime = LocalDateTime.now().format(TIME_FORMATTER);
        log.info("╔════════════════════════════════════════════════════════════╗");
        log.info("║   SCHEDULED IMPORT COMPLETED at {}          ║", endTime);
        log.info("╚════════════════════════════════════════════════════════════╝");
    }

    // ================================
    // MANUAL IMPORT
    // ================================

    /**
     * Manual import trigger (for API or testing)
     */
    public ImportSummary triggerManualImport() {
        String startTime = LocalDateTime.now().format(TIME_FORMATTER);
        log.info("╔════════════════════════════════════════════════════════════╗");
        log.info("║   MANUAL IMPORT TRIGGERED at {}             ║", startTime);
        log.info("╚════════════════════════════════════════════════════════════╝");

        logPlatformConfiguration();

        try {
            ImportSummary summary = executeImport();
            logImportSummary(summary, "MANUAL");
            return summary;
        } catch (Exception e) {
            log.error("❌ Manual import failed: {}", e.getMessage(), e);
            logErrorContext(e);
            return ImportSummary.builder()
                    .startTime(LocalDateTime.now())
                    .endTime(LocalDateTime.now())
                    .status("FAILED")
                    .errorMessage(e.getMessage())
                    .build();
        }
    }

    // ================================
    // CORE EXECUTION
    // ================================

    /**
     * Execute import process with platform awareness
     */
    private ImportSummary executeImport() {
        ImportSummary summary = ImportSummary.createWithDefaultTables();

        try {
            // Step 1: Validate platform configuration
            log.info("📋 Step 1: Validating platform configuration...");
            if (!platformConfig.hasAnyPlatformEnabled()) {
                log.error("❌ No platforms are enabled!");
                log.error("💡 TIP: Enable at least one platform in configuration:");
                log.error("   - Set FACEBOOK_ENABLED=true or");
                log.error("   - Set TIKTOK_ENABLED=true or");
                log.error("   - Set SHOPEE_ENABLED=true");

                summary.setStatus("FAILED");
                summary.setErrorMessage("No platforms enabled");
                return summary;
            }

            log.info("✅ Platform validation passed - {} platform(s) enabled",
                    platformConfig.getEnabledCount());
            log.info("   Active platforms: {}", platformConfig.getEnabledPlatforms());

            // Step 2: Check system readiness
            log.info("📋 Step 2: Checking system readiness...");
            if (!dataCollector.isSystemReady()) {
                log.error("❌ System not ready - APIs unavailable");
                log.error("💡 TIP: Check the logs above for API connectivity issues");
                log.error("   Enabled platforms: {}", platformConfig.getEnabledPlatforms());

                summary.setStatus("FAILED");
                summary.setErrorMessage("APIs unavailable");
                return summary;
            }

            log.info("✅ System is ready - APIs are available");

            // Step 3: Process with pagination and batching
            log.info("📋 Step 3: Starting import for enabled platforms...");
            log.info("   Processing: {}", platformConfig.getEnabledPlatforms());

            ImportSummary processingSummary = apiOrchestrator.collectAndProcessInBatches();

            if (processingSummary != null) {
                summary.merge(processingSummary);
                log.info("✅ Import processing completed");
            } else {
                log.warn("⚠️ Import processing returned null summary");
            }

        } catch (Exception e) {
            log.error("❌ Import execution failed: {}", e.getMessage(), e);
            summary.setStatus("FAILED");
            summary.setErrorMessage(e.getMessage());
        }

        summary.setEndTime(LocalDateTime.now());
        return summary;
    }

    // ================================
    // HEALTH CHECK
    // ================================

    /**
     * Health check for scheduler with platform context
     */
    public boolean isSchedulerHealthy() {
        try {
            // Check if any platforms are enabled
            if (!platformConfig.hasAnyPlatformEnabled()) {
                log.warn("⚠️ Scheduler health check: No platforms enabled");
                return false;
            }

            // Check if system is ready
            boolean healthy = dataCollector.isSystemReady();
            log.debug("Scheduler health check: {} (Platforms: {})",
                    healthy, platformConfig.getEnabledPlatforms());
            return healthy;
        } catch (Exception e) {
            log.warn("❌ Scheduler health check failed: {}", e.getMessage());
            return false;
        }
    }

    // ================================
    // STARTUP HOOK
    // ================================

    /**
     * Application startup hook - log configuration
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        log.info("╔════════════════════════════════════════════════════════════╗");
        log.info("║          APPLICATION READY - SCHEDULER INITIALIZED         ║");
        log.info("╚════════════════════════════════════════════════════════════╝");

        logPlatformConfiguration();

        log.info("📅 Next scheduled import: Check cron expression in config");
        log.info("🔧 To trigger manual import: Call triggerManualImport() endpoint");

        // Uncomment to run import on startup
        // log.info("🚀 Running initial import on startup...");
        // triggerManualImport();
    }

    // ================================
    // LOGGING HELPERS
    // ================================

    private void logPlatformConfiguration() {
        log.info("🔧 Platform Configuration:");
        log.info("   ├─ Facebook: {}", platformConfig.isFacebookEnabled() ? "✅ ENABLED" : "❌ DISABLED");
        log.info("   ├─ TikTok:   {}", platformConfig.isTikTokEnabled() ? "✅ ENABLED" : "❌ DISABLED");
        log.info("   └─ Shopee:   {}", platformConfig.isShopeeEnabled() ? "✅ ENABLED" : "❌ DISABLED");
        log.info("   Total Enabled: {} / 3", platformConfig.getEnabledCount());
    }

    private void logImportSummary(ImportSummary summary, String trigger) {
        log.info("╔════════════════════════════════════════════════════════════╗");
        log.info("║              IMPORT SUMMARY ({})                      ║", String.format("%-10s", trigger));
        log.info("╠════════════════════════════════════════════════════════════╣");

        if (summary != null) {
            log.info("║  Status:         {}", String.format("%-40s", summary.getStatus() != null ? summary.getStatus() : "COMPLETED") + "║");
            log.info("║  Duration:       {}", String.format("%-40s", summary.getDurationFormatted() != null ? summary.getDurationFormatted() : "N/A") + "║");
            log.info("║  API Calls:      {}", String.format("%-40s", summary.getTotalApiCalls()) + "║");
            log.info("║  DB Operations:  {}", String.format("%-40s", summary.getTotalDbOperations()) + "║");

            if (summary.getPlatformCounts() != null && !summary.getPlatformCounts().isEmpty()) {
                log.info("╠════════════════════════════════════════════════════════════╣");
                log.info("║  Platform Data:                                            ║");
                summary.getPlatformCounts().forEach((platform, count) ->
                        log.info("║    ├─ {}: {}", String.format("%-10s", platform), String.format("%-40s", count + " orders") + "║")
                );
            }

            if (summary.getTableInsertCounts() != null && !summary.getTableInsertCounts().isEmpty()) {
                log.info("╠════════════════════════════════════════════════════════════╣");
                log.info("║  Entity Counts:                                            ║");
                summary.getTableInsertCounts().forEach((table, count) ->
                        log.info("║    ├─ {}: {}", String.format("%-10s", table), String.format("%-40s", count + " records") + "║")
                );
            }

            if (summary.getErrorMessage() != null && !summary.getErrorMessage().isEmpty()) {
                log.info("╠════════════════════════════════════════════════════════════╣");
                log.info("║  ⚠️  Error: {}", String.format("%-47s", summary.getErrorMessage()) + "║");
            }
        } else {
            log.info("║  ⚠️  Summary is null                                        ║");
        }

        log.info("╚════════════════════════════════════════════════════════════╝");
    }

    private void logErrorContext(Exception e) {
        log.error("╔════════════════════════════════════════════════════════════╗");
        log.error("║                    ERROR CONTEXT                           ║");
        log.error("╠════════════════════════════════════════════════════════════╣");
        log.error("║  Error Type:    {}", String.format("%-43s", e.getClass().getSimpleName()) + "║");
        log.error("║  Error Message: {}", String.format("%-43s", e.getMessage() != null ? e.getMessage() : "Unknown") + "║");
        log.error("╠════════════════════════════════════════════════════════════╣");
        log.error("║  Platform Config:                                          ║");
        log.error("║    ├─ Enabled Platforms: {}", String.format("%-33s", platformConfig.getEnabledPlatforms()) + "║");
        log.error("║    └─ Total Enabled: {} / 3", String.format("%-35s", platformConfig.getEnabledCount()) + "║");
        log.error("╠════════════════════════════════════════════════════════════╣");
        log.error("║  💡 Troubleshooting Tips:                                  ║");
        log.error("║    1. Check API connectivity for enabled platforms         ║");
        log.error("║    2. Verify .env.production configuration                 ║");
        log.error("║    3. Check database connection                            ║");
        log.error("║    4. Review full stack trace in logs                      ║");
        log.error("╚════════════════════════════════════════════════════════════╝");
    }
}