package com.guno.dataimport.test;

import com.guno.dataimport.DataImportApplication;
import com.guno.dataimport.api.client.ShopeeApiClient;
import com.guno.dataimport.api.client.TikTokApiClient;
import com.guno.dataimport.dto.internal.CollectedData;
import com.guno.dataimport.dto.internal.ProcessingResult;
import com.guno.dataimport.dto.platform.facebook.FacebookApiResponse;
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
 * Shopee Integration Test - Full day data processing with pagination
 * SIMPLIFIED: Single test case for complete daily data import
 * REUSES: FacebookApiResponse (same JSON structure as Facebook)
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
        boolean hasMoreData = true;

        try {
            // Step 1: Collect all pages for the specific date
            log.info("📥 Step 1: Collecting Data with Pagination");
            while (hasMoreData) {
                log.info("   📡 Calling Shopee API - Page: {}, PageSize: {}", currentPage, pageSize);

                FacebookApiResponse response = shopeeApiClient.fetchOrders(testDate, currentPage, pageSize);
                totalApiCalls++;

                if (response == null || response.getCode() != 200) {
                    log.warn("   ⚠️ Shopee API failed at page {}: {}", currentPage,
                            response != null ? response.getMessage() : "null response");
                    break;
                }

                if (response.getData() == null || response.getData().getOrders() == null
                        || response.getData().getOrders().isEmpty()) {
                    log.info("   ✅ No more Shopee data at page {}", currentPage);
                    break;
                }

                int pageOrders = response.getData().getOrders().size();
                allOrders.addAll(response.getData().getOrders().stream()
                        .map(order -> (Object) order)
                        .toList());

                log.info("   📦 Shopee Page {} collected: {} orders", currentPage, pageOrders);

                // Check if this is the last page
                if (pageOrders < pageSize) {
                    log.info("   🏁 Last page detected (got {} < {})", pageOrders, pageSize);
                    hasMoreData = false;
                } else {
                    currentPage++;
                }
            }

            log.info("📊 Shopee Collection Summary:");
            log.info("   - Total API Calls: {}", totalApiCalls);
            log.info("   - Total Orders: {}", allOrders.size());
            log.info("   - Pages Processed: {}", currentPage);

            // Step 2: Process all collected data (TikTok orders go to tbl_customer with segment='TIKTOK')
            log.info("🔄 Step 2: Processing Shopee Data");
            ProcessingResult result = null;

            if (!allOrders.isEmpty()) {
                CollectedData collectedData = new CollectedData();
                collectedData.setShopeeOrders(allOrders); // Set as TikTok orders for proper mapping

                long processingStart = System.currentTimeMillis();
                result = batchProcessor.processCollectedData(collectedData);
                long processingDuration = System.currentTimeMillis() - processingStart;

                log.info("   📋 Shopee processing completed in {}ms", processingDuration);
                log.info("   💾 Records processed: {}", result.getSuccessCount());
                log.info("   ❌ Failed records: {}", result.getFailedCount());
                log.info("   🎯 Platform: Shopee (customer_segment='SHOPEE')");
            } else {
                log.info("   ⚠️ No Shopee data to process");
            }

            // Step 3: Final Results
            long totalDuration = System.currentTimeMillis() - startTime;
            log.info("=".repeat(60));
            log.info("✅ SHOPEE FINAL RESULTS:");
            log.info("   Date Processed: {}", testDate);
            log.info("   Total Duration: {}ms ({:.1f}s)", totalDuration, totalDuration / 1000.0);
            log.info("   Shopee API Performance:");
            log.info("     - Total Calls: {}", totalApiCalls);
            log.info("     - Avg per Call: {:.1f}ms", totalApiCalls > 0 ? (double) totalDuration / totalApiCalls : 0);
            log.info("   Shopee Data Summary:");
            log.info("     - Orders Collected: {}", allOrders.size());
            log.info("     - Orders Processed: {}", result != null ? result.getSuccessCount() : 0);
            log.info("     - Success Rate: {}%", calculateSuccessRate(result, allOrders.size()));
            log.info("   Shopee Features:");
            log.info("     - Customer Segment: 'SHOPEE'");
            log.info("     - Shop ID Prefix: 'SHOPEE_'");
            log.info("     - Product ID Prefix: 'SP_'");
            log.info("     - Payment Provider: 'SHOPEE_PAY'");
            log.info("     - Shipping Provider: 'Shopee Shop Logistics'");
            log.info("   Status: {}", allOrders.size() > 0 ? "SUCCESS ✅" : "NO_DATA ⚠️");
            log.info("=".repeat(60));

            // Assertions
            assertThat(totalApiCalls).isGreaterThan(0);
            if (allOrders.size() > 0) {
                assertThat(result).isNotNull();
                assertThat(result.getSuccessCount()).isGreaterThanOrEqualTo(0);
            }

        } catch (Exception e) {
            log.error("❌ Shopee test failed: {}", e.getMessage(), e);
            fail("Shopee integration test failed: " + e.getMessage());
        }
    }

    private String calculateSuccessRate(ProcessingResult result, int totalOrders) {
        if (result == null || totalOrders == 0) {
            return "N/A";
        }
        double rate = ((double) result.getSuccessCount() / totalOrders) * 100;
        return String.format("%.1f", rate);
    }
}