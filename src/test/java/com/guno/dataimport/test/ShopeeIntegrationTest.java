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
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Shopee Integration Test - WITH EXPLICIT ORDER ID LOGGING
 * ✅ Logs ACTUAL order_id lists (not just counts)
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
        log.info("🎯 SHOPEE FULL DAY DATA TEST - EXPLICIT ORDER ID LOGGING");
        log.info("Date: {} | PageSize: {} | Session: {}", testDate, pageSize, SESSION_ID);
        log.info("=".repeat(60));

        long startTime = System.currentTimeMillis();
        List<Object> allOrders = new ArrayList<>();
        int currentPage = 1;
        int totalApiCalls = 0;
        int totalFilteredOrders = 0;

        // ✅ Track ALL filtered order IDs
        List<String> allFilteredOrderIds = new ArrayList<>();

        boolean hasMoreData = true;

        try {
            // Step 1: Collect all pages
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

                    // ✅ Collect filtered order IDs for THIS PAGE
                    List<String> pageFilteredIds = response.getOrders().stream()
                            .filter(order -> !order.hasShopeeData())
                            .map(ShopeeOrderDto::getOrderIdSafe)
                            .collect(Collectors.toList());

                    allFilteredOrderIds.addAll(pageFilteredIds);

                    // Get valid orders
                    List<ShopeeOrderDto> validOrders = response.getOrders().stream()
                            .filter(ShopeeOrderDto::hasShopeeData)
                            .toList();

                    int failedCount = pageOrderCount - validOrders.size();
                    totalFilteredOrders += failedCount;

                    if (failedCount > 0) {
                        log.warn("   ⚠️ Page {} filtered {} orders", currentPage, failedCount);
                        // ✅ LOG THIS PAGE's FILTERED IDs
                        log.warn("   📋 Page {} Filtered IDs: {}", currentPage, pageFilteredIds);
                    }

                    allOrders.addAll(validOrders);

                    log.info("   📦 Page {} collected: {} orders (Valid: {}, Filtered: {}, Total: {})",
                            currentPage, pageOrderCount, validOrders.size(), failedCount, allOrders.size());

                    if (pageOrderCount < pageSize) {
                        log.info("   ✅ Partial page detected - Last page reached");
                        hasMoreData = false;
                    }

                    currentPage++;
                }

                if (currentPage > 100) {
                    log.warn("   ⚠️ Safety limit reached (100 pages) - Stopping");
                    break;
                }
            }

            long collectionTime = System.currentTimeMillis() - startTime;

            // ✅ COLLECTION SUMMARY
            log.info("════════════════════════════════════════");
            log.info("📊 COLLECTION SUMMARY");
            log.info("════════════════════════════════════════");
            log.info("   Total Orders Received: {}", allOrders.size() + totalFilteredOrders);
            log.info("   Valid Orders: {}", allOrders.size());
            log.info("   Filtered Orders: {}", totalFilteredOrders);
            log.info("   API Calls: {}", totalApiCalls);
            log.info("   Collection Time: {}ms", collectionTime);
            log.info("════════════════════════════════════════");

            // ✅ LOG ALL FILTERED ORDER IDs
            if (!allFilteredOrderIds.isEmpty()) {
                log.warn("════════════════════════════════════════");
                log.warn("📋 ALL FILTERED ORDER IDs (null shopee_data)");
                log.warn("════════════════════════════════════════");
                log.warn("Total: {} orders", allFilteredOrderIds.size());

                if (allFilteredOrderIds.size() <= 100) {
                    // Show all if <= 100
                    log.warn("Order IDs: {}", allFilteredOrderIds);
                } else {
                    // Show first 50 and last 50 if > 100
                    log.warn("First 50: {}", allFilteredOrderIds.subList(0, 50));
                    log.warn("... ({} more orders) ...", allFilteredOrderIds.size() - 100);
                    log.warn("Last 50: {}", allFilteredOrderIds.subList(
                            allFilteredOrderIds.size() - 50, allFilteredOrderIds.size()));
                }
                log.warn("════════════════════════════════════════");
            }

            // Step 2: Process all collected data
            if (!allOrders.isEmpty()) {
                log.info("🔄 Step 2: Processing {} orders", allOrders.size());

                CollectedData collectedData = new CollectedData();
                collectedData.setShopeeOrders(allOrders);

                long processingStartTime = System.currentTimeMillis();
                ProcessingResult result = batchProcessor.processCollectedData(collectedData);
                long processingTime = System.currentTimeMillis() - processingStartTime;

                // ✅ PROCESSING RESULTS
                log.info("════════════════════════════════════════");
                log.info("✅ PROCESSING COMPLETE");
                log.info("════════════════════════════════════════");
                log.info("   Success: {}", result.getSuccessCount());
                log.info("   Failed: {}", result.getFailedCount());
                log.info("   Processing Time: {}ms", processingTime);
                log.info("   Success Rate: {}%", result.getSuccessRate());
                log.info("════════════════════════════════════════");

                // ✅ LOG ERROR DETAILS (grouped)
                if (!result.getErrors().isEmpty()) {
                    log.error("════════════════════════════════════════");
                    log.error("❌ PROCESSING ERRORS");
                    log.error("════════════════════════════════════════");
                    log.error("Total Errors: {}", result.getErrors().size());

                    var errorsByType = result.getErrors().stream()
                            .collect(Collectors.groupingBy(
                                    error -> error.getEntityType() != null ? error.getEntityType() : "UNKNOWN",
                                    Collectors.counting()));

                    errorsByType.forEach((type, count) -> {
                        log.error("   {}: {} errors", type, count);
                    });

                    log.error("Sample errors (first 10):");
                    result.getErrors().stream()
                            .limit(10)
                            .forEach(error -> log.error("   - {}: {} (Order: {})",
                                    error.getEntityType(),
                                    error.getErrorMessage(),
                                    error.getEntityId()));
                    log.error("════════════════════════════════════════");
                }

                // Assertions
                assertThat(result).isNotNull();
                assertThat(result.getTotalProcessed()).isEqualTo(allOrders.size());
                assertThat(result.getSuccessCount()).isGreaterThan(0)
                        .withFailMessage("Expected at least one successful order insertion, but got 0. " +
                                        "Total filtered: %d, Total failed: %d",
                                totalFilteredOrders, result.getFailedCount());
            } else {
                log.warn("⚠️ No orders collected - Skipping processing");
                log.warn("   All {} orders were filtered out", totalFilteredOrders);
            }

            // FINAL SUMMARY
            long totalTime = System.currentTimeMillis() - startTime;
            log.info("════════════════════════════════════════");
            log.info("🎉 TEST COMPLETED");
            log.info("════════════════════════════════════════");
            log.info("Total Time: {}ms", totalTime);
            log.info("Valid Orders: {}", allOrders.size());
            log.info("Filtered Orders: {}", totalFilteredOrders);
            log.info("API Calls: {}", totalApiCalls);
            log.info("════════════════════════════════════════");

        } catch (Exception e) {
            log.error("❌ Test failed with exception", e);
            log.error("📊 Test Failure Summary:");
            log.error("   Total Orders Collected: {}", allOrders.size());
            log.error("   Total Filtered: {}", totalFilteredOrders);
            log.error("   API Calls Made: {}", totalApiCalls);
            log.error("   Exception: {}", e.getMessage());

            fail("Test failed: " + e.getMessage());
        }
    }
}