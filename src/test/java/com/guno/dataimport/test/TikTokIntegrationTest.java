package com.guno.dataimport.test;

import com.guno.dataimport.DataImportApplication;
import com.guno.dataimport.api.client.FacebookApiClient;
import com.guno.dataimport.api.client.ShopeeApiClient;
import com.guno.dataimport.api.client.TikTokApiClient;
import com.guno.dataimport.dto.internal.CollectedData;
import com.guno.dataimport.dto.internal.ProcessingResult;
import com.guno.dataimport.dto.platform.tiktok.TikTokApiResponse;
import com.guno.dataimport.processor.BatchProcessor;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * TikTok Integration Test - UPDATED VERSION
 * Uses TikTokApiResponse instead of FacebookApiResponse
 */
@SpringBootTest(classes = DataImportApplication.class)
@ActiveProfiles("test")
@Slf4j
class TikTokIntegrationTest {

    @Autowired
    private TikTokApiClient tikTokApiClient;

    @Autowired
    private BatchProcessor batchProcessor;

    // Mock các API client không cần thiết
    @MockBean
    private FacebookApiClient facebookApiClient;

    @MockBean
    private ShopeeApiClient shopeeApiClient;

    @Value("${api.tiktok.default-date}")
    private String testDate;

    @Value("${api.tiktok.page-size}")
    private int pageSize;

    private static final String SESSION_ID = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
            .format(LocalDateTime.now());

    @Test
    void shouldProcessFullDayDataWithPagination() {
        log.info("=".repeat(60));
        log.info("🎯 TIKTOK FULL DAY DATA TEST");
        log.info("Date: {} | PageSize: {} | Session: {}", testDate, pageSize, SESSION_ID);
        log.info("=".repeat(60));

        // Mock Facebook và Shopee API để trả về false (không available)
        when(facebookApiClient.isApiAvailable()).thenReturn(false);
        when(shopeeApiClient.isApiAvailable()).thenReturn(false);

        long startTime = System.currentTimeMillis();
        List<Object> allOrders = new ArrayList<>();
        int currentPage = 1;
        int totalApiCalls = 0;
        boolean hasMoreData = true;

        try {
            // Step 1: Collect all pages for the specific date
            log.info("📥 Step 1: Collecting Data with Pagination");
            while (hasMoreData) {
                log.info("   📡 Calling TikTok API - Page: {}, PageSize: {}", currentPage, pageSize);

                // UPDATED: Use TikTokApiResponse instead of FacebookApiResponse
                TikTokApiResponse response = tikTokApiClient.fetchOrders(testDate, currentPage, pageSize);
                totalApiCalls++;

                if (response == null || response.getCode() != 200) {
                    log.warn("   ⚠️ TikTok API failed at page {}: {}", currentPage,
                            response != null ? response.getMessage() : "null response");
                    break;
                }

                // UPDATED: Use TikTokApiResponse methods
                if (!response.hasOrders()) {
                    log.info("   ✅ No more data at page {} - Stopping pagination", currentPage);
                    hasMoreData = false;
                } else {
                    int pageOrderCount = response.getOrderCount();
                    allOrders.addAll(response.getOrders());
                    log.info("   📦 Page {} collected: {} orders (Total: {})",
                            currentPage, pageOrderCount, allOrders.size());

                    // Check if we reached the end
                    if (pageOrderCount < pageSize) {
                        log.info("   ✅ Partial page detected - Last page reached");
                        hasMoreData = false;
                    }

                    currentPage++;
                }

                // Safety limit to prevent infinite loops
                if (currentPage > 100) {
                    log.warn("   ⚠️ Safety limit reached (100 pages) - Stopping");
                    break;
                }
            }

            long collectionTime = System.currentTimeMillis() - startTime;
            log.info("📊 Collection Summary:");
            log.info("   Total Orders: {}", allOrders.size());
            log.info("   API Calls: {}", totalApiCalls);
            log.info("   Collection Time: {}ms", collectionTime);

            // Step 2: Process all collected data
            if (!allOrders.isEmpty()) {
                log.info("🔄 Step 2: Processing {} orders", allOrders.size());

                CollectedData collectedData = new CollectedData();
                collectedData.setTikTokOrders(allOrders);

                long processingStartTime = System.currentTimeMillis();
                ProcessingResult result = batchProcessor.processCollectedData(collectedData);
                long processingTime = System.currentTimeMillis() - processingStartTime;

                log.info("✅ Processing Complete:");
                log.info("   Success: {}", result.getSuccessCount());
                log.info("   Failed: {}", result.getFailedCount());
                log.info("   Processing Time: {}ms", processingTime);
                log.info("   Success Rate: {}%", result.getSuccessRate());

                // Assertions
                assertThat(result).isNotNull();
                assertThat(result.getTotalProcessed()).isEqualTo(allOrders.size());
                assertThat(result.getSuccessCount()).isGreaterThan(0);
            } else {
                log.warn("⚠️ No orders collected - Skipping processing");
            }

            // Final summary
            long totalTime = System.currentTimeMillis() - startTime;
            log.info("=".repeat(60));
            log.info("🎉 TEST COMPLETED");
            log.info("Total Time: {}ms | Orders: {} | API Calls: {}",
                    totalTime, allOrders.size(), totalApiCalls);
            log.info("=".repeat(60));

            // Verify mocks were not called
            verify(facebookApiClient, atMost(1)).isApiAvailable();
            verify(shopeeApiClient, atMost(1)).isApiAvailable();
            verify(facebookApiClient, never()).fetchOrders(anyString(), anyInt(), anyInt());
            verify(shopeeApiClient, never()).fetchOrders(anyString(), anyInt(), anyInt());

        } catch (Exception e) {
            log.error("❌ Test failed with exception", e);
            fail("Test failed: " + e.getMessage());
        }
    }
}