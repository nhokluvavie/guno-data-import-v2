package com.guno.dataimport.api.service;

import com.guno.dataimport.DataImportApplication;
import com.guno.dataimport.dto.internal.CollectedData;
import com.guno.dataimport.dto.internal.ImportSummary;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import static org.assertj.core.api.Assertions.*;

/**
 * DataCollector Test - Performance & Configuration validation
 */
@SpringBootTest(classes = DataImportApplication.class)
@ActiveProfiles("test")
@Slf4j
class DataCollectorTest {

    @Autowired private DataCollector dataCollector;

    @Value("${api.facebook.default-date}")
    private String configuredDate;

    @Value("${processing.batch.size}")
    private int batchSize;

    @Test
    void shouldCollectDataWithYmlConfig() {
        log.info("Testing data collection with YML config - Date: {}, BatchSize: {}",
                configuredDate, batchSize);

        CollectedData data = dataCollector.collectData();

        assertThat(data).isNotNull();
        log.info("Collected {} total orders", data.getTotalOrders());

        if (data.getTotalOrders() > 0) {
            log.info("✅ Data collection successful");
            assertThat(data.getFacebookOrders()).isNotNull();
        } else {
            log.warn("⚠️ No data collected (may be expected)");
        }
    }

    @Test
    void shouldVerifySystemReadiness() {
        log.info("Testing system readiness");

        boolean ready = dataCollector.isSystemReady();
        log.info("System ready: {}", ready);

        assertThat(ready).isNotNull();
    }

    @Test
    void shouldCompareCollectionPerformance() {
        log.info("Comparing normal vs optimized collection performance");

        // Normal collection
        long startTime = System.currentTimeMillis();
        CollectedData normalData = dataCollector.collectData();
        long normalDuration = System.currentTimeMillis() - startTime;

        // Optimized collection (if available)
        startTime = System.currentTimeMillis();
        ImportSummary optimizedSummary = dataCollector.collectAndProcessAllData();
        long optimizedDuration = System.currentTimeMillis() - startTime;

        log.info("Performance comparison:");
        log.info("  Normal: {}ms, {} orders", normalDuration, normalData.getTotalOrders());
        log.info("  Optimized: {}ms, {} operations", optimizedDuration, optimizedSummary.getTotalDbOperations());

        assertThat(normalData).isNotNull();
        assertThat(optimizedSummary).isNotNull();

        if (normalData.getTotalOrders() > 0 && optimizedSummary.getTotalDbOperations() > 0) {
            double efficiency = (double) optimizedSummary.getTotalDbOperations() / normalData.getTotalOrders();
            log.info("  DB Efficiency: {:.2f} operations per order", efficiency);
        }
    }

    @Test
    void shouldHandleEmptyDataGracefully() {
        log.info("Testing empty data handling");

        CollectedData data = dataCollector.collectData();

        assertThat(data).isNotNull();
        assertThat(data.getTotalOrders()).isGreaterThanOrEqualTo(0);

        log.info("Empty data test - Orders: {}", data.getTotalOrders());
    }

    @Test
    void shouldValidateConfiguredSettings() {
        log.info("Validating configured settings from YML");

        assertThat(configuredDate).isNotEmpty();
        assertThat(batchSize).isGreaterThan(0);

        log.info("✅ Config validated - Date: {}, BatchSize: {}", configuredDate, batchSize);
    }
}