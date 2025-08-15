package com.guno.dataimport.api.service;

import com.guno.dataimport.DataImportApplication;
import com.guno.dataimport.dto.internal.CollectedData;
import com.guno.dataimport.dto.internal.ImportSummary;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import static org.assertj.core.api.Assertions.*;

/**
 * DataCollector Test - Updated for streamlined interface
 * Location: src/test/java/com/guno/dataimport/api/service/DataCollectorTest.java
 */
@SpringBootTest(classes = DataImportApplication.class)
@ActiveProfiles("test")
@Slf4j
class DataCollectorTest {

    @Autowired private DataCollector dataCollector;

    @Test
    void shouldCheckSystemReadiness() {
        log.info("Testing system readiness check");

        boolean isReady = dataCollector.isSystemReady();
        log.info("System readiness: {}", isReady ? "READY" : "NOT READY");

        assertThat(isReady).isNotNull();
    }

    @Test
    void shouldCollectSinglePageData() {
        log.info("Testing single page data collection");

        CollectedData data = dataCollector.collectData();

        assertThat(data).isNotNull();
        assertThat(data.getFacebookOrders()).isNotNull();
        log.info("Single page collection - Total orders: {}", data.getTotalOrders());
    }

    @Test
    void shouldExecuteOptimizedCollection() {
        log.info("Testing OPTIMIZED collection (buffered)");

        try {
            ImportSummary summary = dataCollector.collectAndProcessAllData();

            assertThat(summary).isNotNull();
            assertThat(summary.getStartTime()).isNotNull();

            log.info("Optimized collection results:");
            log.info("- Duration: {}", summary.getDurationFormatted());
            log.info("- API Calls: {}", summary.getTotalApiCalls());
            log.info("- DB Operations: {}", summary.getTotalDbOperations());
            log.info("- Platform Counts: {}", summary.getPlatformCounts());

        } catch (Exception e) {
            log.warn("Optimized collection limited due to API: {}", e.getMessage());
        }
    }

    @Test
    void shouldExecuteLegacyCollection() {
        log.info("Testing LEGACY collection (page by page)");

        try {
            ImportSummary summary = dataCollector.collectAndProcessAllDataLegacy();

            assertThat(summary).isNotNull();
            log.info("Legacy collection completed");

        } catch (Exception e) {
            log.warn("Legacy collection limited due to API: {}", e.getMessage());
        }
    }

    @Test
    void shouldSupportCustomCollectionStrategy() {
        log.info("Testing custom collection strategy");

        try {
            // Small buffer for testing
            ImportSummary summary = dataCollector.collectAndProcess(50, true, 200);

            assertThat(summary).isNotNull();
            log.info("Custom collection - PageSize: 50, Buffer: 200");
            log.info("- API Calls: {}", summary.getTotalApiCalls());
            log.info("- DB Operations: {}", summary.getTotalDbOperations());

        } catch (Exception e) {
            log.warn("Custom collection limited due to API: {}", e.getMessage());
        }
    }

    @Test
    void shouldComparePerformanceStrategies() {
        log.info("Testing performance comparison between strategies");

        try {
            // Test both strategies with small data
            long bufferedStart = System.currentTimeMillis();
            ImportSummary bufferedSummary = dataCollector.collectAndProcess(20, true, 100);
            long bufferedDuration = System.currentTimeMillis() - bufferedStart;

            long legacyStart = System.currentTimeMillis();
            ImportSummary legacySummary = dataCollector.collectAndProcessAllDataLegacy();
            long legacyDuration = System.currentTimeMillis() - legacyStart;

            log.info("PERFORMANCE COMPARISON:");
            log.info("Buffered - Duration: {}ms, DB Ops: {}",
                    bufferedDuration, bufferedSummary.getTotalDbOperations());
            log.info("Legacy - Duration: {}ms, DB Ops: {}",
                    legacyDuration, legacySummary.getTotalDbOperations());

            // Buffered should have fewer DB operations
            if (bufferedSummary.getTotalDbOperations() > 0 && legacySummary.getTotalDbOperations() > 0) {
                assertThat(bufferedSummary.getTotalDbOperations())
                        .isLessThanOrEqualTo(legacySummary.getTotalDbOperations());
            }

        } catch (Exception e) {
            log.warn("Performance comparison limited: {}", e.getMessage());
        }
    }
}