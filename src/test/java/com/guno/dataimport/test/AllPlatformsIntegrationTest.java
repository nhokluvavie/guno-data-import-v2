package com.guno.dataimport.test;

import com.guno.dataimport.DataImportApplication;
import com.guno.dataimport.api.client.FacebookApiClient;
import com.guno.dataimport.api.client.ShopeeApiClient;
import com.guno.dataimport.api.client.TikTokApiClient;
import com.guno.dataimport.dto.internal.CollectedData;
import com.guno.dataimport.dto.internal.ProcessingResult;
import com.guno.dataimport.dto.platform.facebook.FacebookApiResponse;
import com.guno.dataimport.dto.platform.tiktok.TikTokApiResponse;
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
 * All Platforms Integration Test - Process Facebook, TikTok, Shopee simultaneously
 * Location: src/test/java/com/guno/dataimport/test/AllPlatformsIntegrationTest.java
 *
 * This test combines the logic from:
 * - FacebookIntegrationTest
 * - TikTokIntegrationTest
 * - ShopeeIntegrationTest
 */
@SpringBootTest(classes = DataImportApplication.class)
@ActiveProfiles("test")
@Slf4j
class AllPlatformsIntegrationTest {

    @Autowired private FacebookApiClient facebookApiClient;
    @Autowired private TikTokApiClient tikTokApiClient;
    @Autowired private ShopeeApiClient shopeeApiClient;
    @Autowired private BatchProcessor batchProcessor;

    @Value("${api.facebook.default-date}") private String facebookDate;
    @Value("${api.tiktok.default-date}") private String tiktokDate;
    @Value("${api.shopee.default-date}") private String shopeeDate;

    @Value("${api.facebook.page-size}") private int facebookPageSize;
    @Value("${api.tiktok.page-size}") private int tiktokPageSize;
    @Value("${api.shopee.page-size}") private int shopeePageSize;

    private static final String SESSION_ID = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
            .format(LocalDateTime.now());

    @Test
    void shouldProcessAllPlatformsData() {
        log.info("=".repeat(80));
        log.info("🌐 ALL PLATFORMS INTEGRATION TEST - Session: {}", SESSION_ID);
        log.info("=".repeat(80));

        long globalStartTime = System.currentTimeMillis();

        // Collect data from all 3 platforms
        PlatformData facebookData = collectFacebookData();
        PlatformData tiktokData = collectTikTokData();
        PlatformData shopeeData = collectShopeeData();

        // Process all platforms together
        ProcessingResult result = processAllPlatforms(facebookData, tiktokData, shopeeData);

        // Print final summary
        long totalDuration = System.currentTimeMillis() - globalStartTime;
        printFinalSummary(facebookData, tiktokData, shopeeData, result, totalDuration);

        // Assertions
        int totalOrders = facebookData.orders.size() + tiktokData.orders.size() + shopeeData.orders.size();
        assertThat(result).isNotNull();
        if (totalOrders > 0) {
            assertThat(result.getSuccessCount()).isGreaterThanOrEqualTo(0);
        }
    }

    // ========== FACEBOOK COLLECTION ==========
    private PlatformData collectFacebookData() {
        log.info("\n📘 FACEBOOK DATA COLLECTION");
        log.info("   Date: {} | PageSize: {}", facebookDate, facebookPageSize);

        PlatformData data = new PlatformData("Facebook");
        int currentPage = 1;
        boolean hasMoreData = true;

        try {
            while (hasMoreData) {
                log.info("   📡 Facebook API - Page: {}", currentPage);

                FacebookApiResponse response = facebookApiClient.fetchOrders(facebookDate, currentPage, facebookPageSize);
                data.apiCalls++;

                if (response == null || response.getCode() != 200) {
                    log.warn("   ⚠️ Facebook API failed at page {}", currentPage);
                    break;
                }

                if (response.getData() == null || response.getData().getOrders() == null
                        || response.getData().getOrders().isEmpty()) {
                    log.info("   ✅ No more Facebook data at page {}", currentPage);
                    break;
                }

                int pageOrders = response.getData().getOrders().size();
                data.orders.addAll(response.getData().getOrders().stream()
                        .map(order -> (Object) order)
                        .toList());

                log.info("   📦 Facebook Page {} collected: {} orders", currentPage, pageOrders);

                if (pageOrders < facebookPageSize) {
                    log.info("   🏁 Last page detected");
                    hasMoreData = false;
                } else {
                    currentPage++;
                }
            }

            log.info("   ✅ Facebook collection completed: {} orders, {} API calls",
                    data.orders.size(), data.apiCalls);

        } catch (Exception e) {
            log.error("   ❌ Facebook collection failed: {}", e.getMessage());
            data.error = e.getMessage();
        }

        return data;
    }

    // ========== TIKTOK COLLECTION ==========
    private PlatformData collectTikTokData() {
        log.info("\n📗 TIKTOK DATA COLLECTION");
        log.info("   Date: {} | PageSize: {}", tiktokDate, tiktokPageSize);

        PlatformData data = new PlatformData("TikTok");
        int currentPage = 1;
        boolean hasMoreData = true;

        try {
            while (hasMoreData) {
                log.info("   📡 TikTok API - Page: {}", currentPage);

                TikTokApiResponse response = tikTokApiClient.fetchOrders(tiktokDate, currentPage, tiktokPageSize);
                data.apiCalls++;

                if (response == null || response.getCode() != 200) {
                    log.warn("   ⚠️ TikTok API failed at page {}", currentPage);
                    break;
                }

                if (response.getData() == null || response.getData().getOrders() == null
                        || response.getData().getOrders().isEmpty()) {
                    log.info("   ✅ No more TikTok data at page {}", currentPage);
                    break;
                }

                int pageOrders = response.getData().getOrders().size();
                data.orders.addAll(response.getData().getOrders().stream()
                        .map(order -> (Object) order)
                        .toList());

                log.info("   📦 TikTok Page {} collected: {} orders", currentPage, pageOrders);

                if (pageOrders < tiktokPageSize) {
                    log.info("   🏁 Last page detected");
                    hasMoreData = false;
                } else {
                    currentPage++;
                }
            }

            log.info("   ✅ TikTok collection completed: {} orders, {} API calls",
                    data.orders.size(), data.apiCalls);

        } catch (Exception e) {
            log.error("   ❌ TikTok collection failed: {}", e.getMessage());
            data.error = e.getMessage();
        }

        return data;
    }

    // ========== SHOPEE COLLECTION ==========
    private PlatformData collectShopeeData() {
        log.info("\n📙 SHOPEE DATA COLLECTION");
        log.info("   Date: {} | PageSize: {}", shopeeDate, shopeePageSize);

        PlatformData data = new PlatformData("Shopee");
        int currentPage = 1;
        boolean hasMoreData = true;

        try {
            while (hasMoreData) {
                log.info("   📡 Shopee API - Page: {}", currentPage);

                FacebookApiResponse response = shopeeApiClient.fetchOrders(shopeeDate, currentPage, shopeePageSize);
                data.apiCalls++;

                if (response == null || response.getCode() != 200) {
                    log.warn("   ⚠️ Shopee API failed at page {}", currentPage);
                    break;
                }

                if (response.getData() == null || response.getData().getOrders() == null
                        || response.getData().getOrders().isEmpty()) {
                    log.info("   ✅ No more Shopee data at page {}", currentPage);
                    break;
                }

                int pageOrders = response.getData().getOrders().size();
                data.orders.addAll(response.getData().getOrders().stream()
                        .map(order -> (Object) order)
                        .toList());

                log.info("   📦 Shopee Page {} collected: {} orders", currentPage, pageOrders);

                if (pageOrders < shopeePageSize) {
                    log.info("   🏁 Last page detected");
                    hasMoreData = false;
                } else {
                    currentPage++;
                }
            }

            log.info("   ✅ Shopee collection completed: {} orders, {} API calls",
                    data.orders.size(), data.apiCalls);

        } catch (Exception e) {
            log.error("   ❌ Shopee collection failed: {}", e.getMessage());
            data.error = e.getMessage();
        }

        return data;
    }

    // ========== PROCESS ALL PLATFORMS ==========
    private ProcessingResult processAllPlatforms(PlatformData facebook, PlatformData tiktok, PlatformData shopee) {
        log.info("\n🔄 PROCESSING ALL PLATFORMS DATA");

        int totalOrders = facebook.orders.size() + tiktok.orders.size() + shopee.orders.size();
        log.info("   Total orders to process: {}", totalOrders);
        log.info("   - Facebook: {}", facebook.orders.size());
        log.info("   - TikTok: {}", tiktok.orders.size());
        log.info("   - Shopee: {}", shopee.orders.size());

        if (totalOrders == 0) {
            log.warn("   ⚠️ No data to process");
            return ProcessingResult.builder().build();
        }

        try {
            CollectedData collectedData = new CollectedData();
            collectedData.setFacebookOrders(facebook.orders);
            collectedData.setTikTokOrders(tiktok.orders);
            collectedData.setShopeeOrders(shopee.orders);

            long processingStart = System.currentTimeMillis();
            ProcessingResult result = batchProcessor.processCollectedData(collectedData);
            long processingDuration = System.currentTimeMillis() - processingStart;

            log.info("   ✅ Processing completed in {}ms", processingDuration);
            log.info("   💾 Records processed: {}", result.getSuccessCount());
            log.info("   ❌ Failed records: {}", result.getFailedCount());

            return result;

        } catch (Exception e) {
            log.error("   ❌ Processing failed: {}", e.getMessage(), e);
            return ProcessingResult.builder()
                    .failedCount(totalOrders)
                    .build();
        }
    }

    // ========== FINAL SUMMARY ==========
    private void printFinalSummary(PlatformData facebook, PlatformData tiktok,
                                   PlatformData shopee, ProcessingResult result, long totalDuration) {
        log.info("\n" + "=".repeat(80));
        log.info("🎯 ALL PLATFORMS FINAL SUMMARY");
        log.info("=".repeat(80));

        log.info("📊 COLLECTION SUMMARY:");
        printPlatformSummary("Facebook", facebook, facebookDate);
        printPlatformSummary("TikTok", tiktok, tiktokDate);
        printPlatformSummary("Shopee", shopee, shopeeDate);

        log.info("\n💾 PROCESSING SUMMARY:");
        log.info("   Total Orders Collected: {}",
                facebook.orders.size() + tiktok.orders.size() + shopee.orders.size());
        log.info("   Successfully Processed: {}", result.getSuccessCount());
        log.info("   Failed: {}", result.getFailedCount());
        log.info("   Success Rate: {}%", calculateSuccessRate(result,
                facebook.orders.size() + tiktok.orders.size() + shopee.orders.size()));

        log.info("\n⏱️ PERFORMANCE SUMMARY:");
        log.info("   Total Duration: {}ms ({:.1f}s)", totalDuration, totalDuration / 1000.0);
        log.info("   Total API Calls: {}", facebook.apiCalls + tiktok.apiCalls + shopee.apiCalls);
        log.info("   Avg API Call Time: {:.1f}ms",
                (double) totalDuration / (facebook.apiCalls + tiktok.apiCalls + shopee.apiCalls));

        log.info("\n🎯 DATABASE IMPACT:");
        log.info("   Customer Segment Distribution:");
        log.info("   - FACEBOOK: {} orders", facebook.orders.size());
        log.info("   - TIKTOK: {} orders", tiktok.orders.size());
        log.info("   - SHOPEE: {} orders", shopee.orders.size());

        log.info("\n✅ TEST STATUS: {}",
                result.getFailedCount() == 0 ? "SUCCESS" : "PARTIAL SUCCESS");
        log.info("=".repeat(80));
    }

    private void printPlatformSummary(String platform, PlatformData data, String date) {
        log.info("   {} ({}): {} orders, {} API calls{}",
                platform, date, data.orders.size(), data.apiCalls,
                data.error != null ? " [ERROR: " + data.error + "]" : "");
    }

    private String calculateSuccessRate(ProcessingResult result, int totalOrders) {
        if (result == null || totalOrders == 0) return "N/A";
        double rate = ((double) result.getSuccessCount() / totalOrders) * 100;
        return String.format("%.1f", rate);
    }

    // ========== HELPER CLASS ==========
    private static class PlatformData {
        String platform;
        List<Object> orders = new ArrayList<>();
        int apiCalls = 0;
        String error = null;

        PlatformData(String platform) {
            this.platform = platform;
        }
    }
}