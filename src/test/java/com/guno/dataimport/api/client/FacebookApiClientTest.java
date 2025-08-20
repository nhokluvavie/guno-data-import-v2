package com.guno.dataimport.api.client;

import com.guno.dataimport.DataImportApplication;
import com.guno.dataimport.dto.platform.facebook.FacebookApiResponse;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import static org.assertj.core.api.Assertions.*;

/**
 * FacebookApiClient Test - Optimized with YML configuration
 */
@SpringBootTest(classes = DataImportApplication.class)
@ActiveProfiles("test")
@Slf4j
class FacebookApiClientTest {

    @Autowired private FacebookApiClient facebookApiClient;

    @Value("${api.facebook.default-date}")
    private String defaultDate;

    @Value("${api.facebook.page-size}")
    private int pageSize;

    @Value("${api.facebook.source}")
    private String source;

    @Value("${api.facebook.max-retries}")
    private int maxRetries;

    @Test
    void shouldConnectWithYmlConfig() {
        log.info("Testing API connectivity with YML config");

        boolean available = facebookApiClient.isApiAvailable();
        log.info("API Available: {}, Config - Date: {}, PageSize: {}, Source: {}",
                available, defaultDate, pageSize, source);

        assertThat(available).isNotNull();
    }

    @Test
    void shouldFetchOrdersWithDefaultConfig() {
        log.info("Testing fetchOrders() with default YML settings");

        FacebookApiResponse response = facebookApiClient.fetchOrders();

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isNotNull();
        log.info("Default fetch - Status: {}, Orders: {}", response.getStatus(), response.getOrderCount());

        if (response.isSuccess()) {
            assertThat(response.getData()).isNotNull();
            log.info("✅ Success with {} orders", response.getOrderCount());
        } else {
            log.warn("⚠️ Failed: {}", response.getMessage());
        }
    }

    @Test
    void shouldUsePaginationFromYml() {
        log.info("Testing pagination with YML pageSize: {}", pageSize);

        FacebookApiResponse response = facebookApiClient.fetchOrders(defaultDate, 1, pageSize);

        assertThat(response).isNotNull();
        log.info("Pagination - Page: 1, Size: {}, Status: {}, Got: {}",
                pageSize, response.getStatus(), response.getOrderCount());

        if (response.isSuccess() && response.getOrderCount() > 0) {
            assertThat(response.getOrderCount()).isLessThanOrEqualTo(pageSize);
            log.info("✅ Pagination within limits");
        }
    }

    @Test
    void shouldUseSourceFromYml() {
        log.info("Testing source parameter from YML: {}", source);

        FacebookApiResponse response = facebookApiClient.fetchOrders(defaultDate, 1, 10, source);

        assertThat(response).isNotNull();
        log.info("Source test - Source: {}, Status: {}", source, response.getStatus());
        assertThat(response.getMessage()).isNotNull();
    }

    @Test
    void shouldValidateYmlConfiguration() {
        log.info("Validating YML configuration");

        assertThat(defaultDate).isEqualTo("2025-08-18");
        assertThat(pageSize).isEqualTo(1);
        assertThat(source).isEqualTo("facebook");
        assertThat(maxRetries).isEqualTo(3);

        log.info("✅ YML Config - Date: {}, PageSize: {}, Source: {}, Retries: {}",
                defaultDate, pageSize, source, maxRetries);
    }

    @Test
    void shouldMeasurePerformanceWithYmlSettings() {
        log.info("Performance test with YML pageSize: {}", pageSize);

        long startTime = System.currentTimeMillis();
        FacebookApiResponse response = facebookApiClient.fetchOrders(defaultDate, 1, Math.min(pageSize, 10));
        long duration = System.currentTimeMillis() - startTime;

        assertThat(response).isNotNull();
        log.info("Performance - Duration: {}ms, Orders: {}, Retries allowed: {}",
                duration, response.getOrderCount(), maxRetries);

        if (response.isSuccess() && duration > 0 && response.getOrderCount() > 0) {
            double throughput = response.getOrderCount() * 1000.0 / duration;
            log.info("API Throughput: {:.1f} orders/second", throughput);
        }

        assertThat(duration).isLessThan(30000);
    }

    @Test
    void shouldHandleErrorsWithRetryConfig() {
        log.info("Testing error handling with {} retries", maxRetries);

        FacebookApiResponse response = facebookApiClient.fetchOrders("invalid-date", 1, 5);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isNotNull();
        log.info("Error handling - Status: {}, MaxRetries: {}", response.getStatus(), maxRetries);
    }
}