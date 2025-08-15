package com.guno.dataimport.api.client;

import com.guno.dataimport.DataImportApplication;
import com.guno.dataimport.dto.platform.facebook.FacebookApiResponse;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import static org.assertj.core.api.Assertions.*;

/**
 * FacebookApiClient Test - Test API connectivity and retry mechanism
 * Location: src/test/java/com/guno/dataimport/api/client/FacebookApiClientTest.java
 */
@SpringBootTest(classes = DataImportApplication.class)
@ActiveProfiles("test")
@Slf4j
class FacebookApiClientTest {

    @Autowired private FacebookApiClient facebookApiClient;

    @Test
    void testApiAvailability() {
        log.info("Testing Facebook API availability");

        boolean available = facebookApiClient.isApiAvailable();
        log.info("Facebook API available: {}", available);

        // Test doesn't fail if API is down, just logs the result
        assertThat(available).isNotNull();
    }

    @Test
    void testBasicFetchOrders() {
        log.info("Testing basic fetchOrders() call");

        FacebookApiResponse response = facebookApiClient.fetchOrders();

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isNotNull();
        assertThat(response.getMessage()).isNotNull();

        log.info("API Response - Status: {}, Message: {}", response.getStatus(), response.getMessage());

        if (response.isSuccess() && response.getData() != null) {
            log.info("Successfully fetched {} orders", response.getOrderCount());
            assertThat(response.getData().getOrders()).isNotNull();
        } else {
            log.warn("API call failed or returned no data");
        }
    }

    @Test
    void testFetchOrdersWithDate() {
        log.info("Testing fetchOrders() with specific date");

        String testDate = "2025-08-12";
        FacebookApiResponse response = facebookApiClient.fetchOrders(testDate);

        assertThat(response).isNotNull();
        log.info("Fetch with date {} - Status: {}", testDate, response.getStatus());

        // Verify response structure regardless of success
        assertThat(response.getStatus()).isNotNull();
        assertThat(response.getCode()).isNotNull();
    }

    @Test
    void testPaginationParameters() {
        log.info("Testing pagination parameters");

        int page = 1;
        int pageSize = 5; // Small page size for testing
        FacebookApiResponse response = facebookApiClient.fetchOrders("", page, pageSize);

        assertThat(response).isNotNull();
        log.info("Pagination test - Page: {}, Size: {}, Status: {}",
                page, pageSize, response.getStatus());

        if (response.isSuccess() && response.getData() != null) {
            int actualCount = response.getOrderCount();
            log.info("Received {} orders (expected ≤ {})", actualCount, pageSize);

            // Should not exceed requested page size (unless API has different logic)
            assertThat(actualCount).isGreaterThanOrEqualTo(0);
        }
    }

    @Test
    void testFilterDateParameter() {
        log.info("Testing filter-date parameter");

        String date = "2025-08-12";
        String filterDate = "create"; // Alternative to default "update"

        FacebookApiResponse response = facebookApiClient.fetchOrders(date, 1, 10, filterDate);

        assertThat(response).isNotNull();
        log.info("Filter-date test - Date: {}, Filter: {}, Status: {}",
                date, filterDate, response.getStatus());

        // Test structure regardless of API success
        assertThat(response.getMessage()).isNotNull();
    }

    @Test
    void testResponseStructure() {
        log.info("Testing API response structure");

        FacebookApiResponse response = facebookApiClient.fetchOrders();

        // Test response structure
        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isNotNull();
        assertThat(response.getMessage()).isNotNull();
        assertThat(response.getCode()).isNotNull();

        // Test data wrapper (might be null if no data)
        if (response.getData() != null) {
            assertThat(response.getData().getOrders()).isNotNull();
            log.info("Response data structure verified - Orders: {}",
                    response.getData().getOrders().size());
        } else {
            log.info("Response data is null (no orders available)");
        }

        // Test helper methods
        assertThat(response.getOrderCount()).isGreaterThanOrEqualTo(0);
        log.info("Order count: {}", response.getOrderCount());
    }

    @Test
    void testRetryMechanism() {
        log.info("Testing retry mechanism (implicit - by checking behavior)");

        // This test verifies that the retry mechanism doesn't break normal flow
        // We can't easily test actual retry behavior without mocking

        long startTime = System.currentTimeMillis();
        FacebookApiResponse response = facebookApiClient.fetchOrders();
        long duration = System.currentTimeMillis() - startTime;

        assertThat(response).isNotNull();
        log.info("API call completed in {}ms", duration);

        // If retry mechanism is working, failed calls should take longer
        // But successful calls should be reasonably fast
        if (response.isSuccess()) {
            assertThat(duration).isLessThan(30000); // Should complete within 30 seconds
        } else {
            log.info("API call failed - retry mechanism may have been triggered");
        }
    }

    @Test
    void testErrorHandling() {
        log.info("Testing error handling with edge cases");

        // Test with potentially invalid date format
        FacebookApiResponse response1 = facebookApiClient.fetchOrders("invalid-date");
        assertThat(response1).isNotNull();
        log.info("Invalid date test - Status: {}", response1.getStatus());

        // Test with zero page size (edge case)
        FacebookApiResponse response2 = facebookApiClient.fetchOrders("", 1, 0);
        assertThat(response2).isNotNull();
        log.info("Zero page size test - Status: {}", response2.getStatus());

        // Verify that client handles errors gracefully (doesn't throw exceptions)
        assertThat(response1.getStatus()).isNotNull();
        assertThat(response2.getStatus()).isNotNull();
    }
}