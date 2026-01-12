package com.guno.dataimport.test;

import com.guno.dataimport.DataImportApplication;
import com.guno.dataimport.api.client.ShopeeApiClient;
import com.guno.dataimport.dto.internal.CollectedData;
import com.guno.dataimport.dto.internal.ProcessingResult;
import com.guno.dataimport.dto.platform.shopee.ShopeeApiResponse;
import com.guno.dataimport.dto.platform.shopee.ShopeeOrderDto;
import com.guno.dataimport.processor.BatchProcessor;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Shopee Integration Test - CLEAN VERSION
 * ✅ Uses ShopeeApiResponse (not FacebookApiResponse)
 * Pattern: Identical to TikTokIntegrationTest
 */
@SpringBootTest(classes = DataImportApplication.class)
@ActiveProfiles("test")
@Slf4j
class ShopeeIntegrationTest {

    @Autowired
    private ShopeeApiClient shopeeApiClient;

    @Autowired
    private BatchProcessor batchProcessor;

    @Value("${api.shopee.default-date}")
    private String testDate;

    @Value("${api.shopee.page-size}")
    private int pageSize;

    private static final String SESSION_ID = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
            .format(LocalDateTime.now());

    @Test
    void shouldProcessFullDayDataWithPagination() {
        log.info("=".repeat(60));
        log.info("🎯 SHOPEE FULL DAY DATA TEST");
        log.info("Date: {} | PageSize: {} | Session: {}", testDate, pageSize, SESSION_ID);
        log.info("=".repeat(60));

        long startTime = System.currentTimeMillis();
        List<Object> allOrders = new ArrayList<>();
        int currentPage = 1;
        int totalApiCalls = 0;
        int filteredOrders = 0;
        boolean hasMoreData = true;

        try {
            // Step 1: Collect all pages for the specific date
            log.info("📥 Step 1: Collecting Data with Pagination");
            while (hasMoreData) {
                log.info("   📡 Calling Shopee API - Page: {}, PageSize: {}", currentPage, pageSize);

                ShopeeApiResponse response = shopeeApiClient.fetchOrders(testDate, currentPage, pageSize);
                totalApiCalls++;

                if (response == null || response.getCode() != 200) {
                    log.warn("   ⚠️ Shopee API failed at page {}: {}", currentPage,
                            response != null ? response.getMessage() : "null response");
                    break;
                }

                if (!response.hasOrders()) {
                    log.info("   ✅ No more data at page {} - Stopping pagination", currentPage);
                    hasMoreData = false;
                } else {
                    int pageOrderCount = response.getOrderCount();

                    // ✅ CLEAN: Direct access to ShopeeOrderDto
                    List<ShopeeOrderDto> validOrders = response.getOrders().stream()
                            .filter(ShopeeOrderDto::hasShopeeData)  // Filter out null shopee_data
                            .toList();

                    int failedCount = pageOrderCount - validOrders.size();
                    if (failedCount > 0) {
                        log.warn("   ⚠️ Filtered out {} orders with null shopee_data", failedCount);
                        filteredOrders += failedCount;
                    }

                    allOrders.addAll(validOrders);

                    log.info("   📦 Page {} collected: {} orders (Valid: {}, Filtered: {}, Total: {})",
                            currentPage, pageOrderCount, validOrders.size(), failedCount, allOrders.size());

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
            log.info("   Filtered: {}", filteredOrders);
            log.info("   API Calls: {}", totalApiCalls);
            log.info("   Collection Time: {}ms", collectionTime);

            // Step 2: Process all collected data
            if (!allOrders.isEmpty()) {
                log.info("🔄 Step 2: Processing {} orders", allOrders.size());

                CollectedData collectedData = new CollectedData();
                collectedData.setShopeeOrders(allOrders);

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

        } catch (Exception e) {
            log.error("❌ Test failed with exception", e);
            fail("Test failed: " + e.getMessage());
        }
    }
}