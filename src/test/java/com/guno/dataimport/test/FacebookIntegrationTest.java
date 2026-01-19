package com.guno.dataimport.test;

import com.guno.dataimport.DataImportApplication;
import com.guno.dataimport.api.client.FacebookApiClient;
import com.guno.dataimport.dto.internal.CollectedData;
import com.guno.dataimport.dto.internal.ProcessingResult;
import com.guno.dataimport.dto.platform.facebook.FacebookApiResponse;
import com.guno.dataimport.dto.platform.facebook.FacebookOrderDto;
import com.guno.dataimport.processor.BatchProcessor;
import com.guno.dataimport.test.util.DateRangeTestHelper;
import com.guno.dataimport.test.util.DateRangeSummary;
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

import static org.assertj.core.api.Assertions.*;

/**
 * Facebook Integration Test - WITH DATE RANGE SUPPORT
 */
@SpringBootTest(classes = DataImportApplication.class)
@ActiveProfiles("test")
@Slf4j
class FacebookIntegrationTest {

    @Autowired private FacebookApiClient facebookApiClient;
    @Autowired private BatchProcessor batchProcessor;
    @Autowired private DateRangeTestHelper dateRangeHelper;

    @Value("${api.facebook.page-size}")
    private int pageSize;

    private static final String SESSION_ID = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
            .format(LocalDateTime.now());

    @Test
    void shouldProcessDateRangeWithPagination() {
        // Validate configuration
        dateRangeHelper.validateConfiguration();

        // Get dates to process
        List<String> datesToProcess = dateRangeHelper.getDatesToProcess();
        DateRangeSummary summary = new DateRangeSummary("FACEBOOK");

        log.info("╔═══════════════════════════════════════════════════════════════════╗");
        log.info("║  🎯 FACEBOOK DATE RANGE PROCESSING TEST");
        log.info("╠═══════════════════════════════════════════════════════════════════╣");
        log.info("║  {}", dateRangeHelper.getDateRangeInfo());
        log.info("║  Page Size: {} | Session: {}", pageSize, SESSION_ID);
        log.info("║  Continue on Error: {}", dateRangeHelper.shouldContinueOnError());
        log.info("╚═══════════════════════════════════════════════════════════════════╝");

        // Process each date
        int dateIndex = 1;
        for (String currentDate : datesToProcess) {
            log.info("");
            log.info("═".repeat(70));
            log.info("📅 Processing Date {}/{}: {}", dateIndex, datesToProcess.size(), currentDate);
            log.info("═".repeat(70));

            long dateStartTime = System.currentTimeMillis();
            boolean dateSuccess = false;
            String errorMessage = null;
            int ordersCollected = 0;
            int successCount = 0;
            int failedCount = 0;

            try {
                // Process single date
                ProcessingResult result = processSingleDate(currentDate);

                ordersCollected = result.getTotalProcessed();
                successCount = result.getSuccessCount();
                failedCount = result.getFailedCount();
                dateSuccess = result.getSuccessCount() > 0;

                // Log per-date summary if enabled
                if (dateRangeHelper.shouldLogPerDateSummary()) {
                    logDateSummary(currentDate, result, System.currentTimeMillis() - dateStartTime);
                }

            } catch (Exception e) {
                log.error("❌ Error processing date {}: {}", currentDate, e.getMessage(), e);
                errorMessage = e.getMessage();

                if (!dateRangeHelper.shouldContinueOnError()) {
                    log.error("🛑 Stopping due to error (continue-on-error=false)");
                    summary.addDateResult(currentDate, ordersCollected, successCount, failedCount,
                            System.currentTimeMillis() - dateStartTime, false, errorMessage);
                    break;
                }
            }

            // Add result to summary
            summary.addDateResult(currentDate, ordersCollected, successCount, failedCount,
                    System.currentTimeMillis() - dateStartTime, dateSuccess, errorMessage);

            // Delay between dates
            if (dateIndex < datesToProcess.size()) {
                dateRangeHelper.delayBetweenDates();
            }

            dateIndex++;
        }

        // Print comprehensive summary
        log.info("");
        if (dateRangeHelper.shouldGenerateSummary()) {
            summary.printSummary();
        }

        // Assertions
        assertThat(summary.getTotalOrders()).isGreaterThanOrEqualTo(0);

        if (!dateRangeHelper.shouldContinueOnError()) {
            assertThat(summary.isAllSuccessful())
                    .withFailMessage("Some dates failed: " + summary.getFailedDates())
                    .isTrue();
        }
    }

    /**
     * Process a single date
     */
    private ProcessingResult processSingleDate(String date) {
        List<FacebookOrderDto> allOrders = new ArrayList<>();
        int currentPage = 1;
        boolean hasMoreData = true;

        log.info("📥 Step 1: Collecting Data for {}", date);

        // Collect all pages
        while (hasMoreData) {
            log.info("   📡 Calling Facebook API - Date: {}, Page: {}, PageSize: {}",
                    date, currentPage, pageSize);

            FacebookApiResponse response = facebookApiClient.fetchOrders(date, currentPage, pageSize);

            if (response == null || response.getCode() != 200) {
                log.warn("   ⚠️ API failed at page {}: {}", currentPage,
                        response != null ? response.getMessage() : "null response");
                break;
            }

            List<FacebookOrderDto> pageOrders = response.getData() != null ?
                    response.getData().getOrders() : null;

            if (pageOrders == null || pageOrders.isEmpty()) {
                log.info("   ✅ No more data at page {}", currentPage);
                break;
            }

            allOrders.addAll(pageOrders);
            log.info("   📦 Page {} collected: {} orders (Total so far: {})",
                    currentPage, pageOrders.size(), allOrders.size());

            // Check if last page
            if (pageOrders.size() < pageSize) {
                log.info("   ✅ Partial page detected - Last page reached");
                hasMoreData = false;
            } else {
                currentPage++;
            }
        }

        log.info("📊 Collection Summary for {}: {} orders collected", date, allOrders.size());

        // Process collected data
        if (allOrders.isEmpty()) {
            log.warn("⚠️ No valid orders for date {}, skipping processing", date);
            return new ProcessingResult();
        }

        log.info("🔄 Step 2: Processing {} orders for {}", allOrders.size(), date);

        CollectedData collectedData = new CollectedData();
        collectedData.setFacebookOrders(new ArrayList<>(allOrders));

        ProcessingResult result = batchProcessor.processCollectedData(collectedData);

        return result;
    }

    /**
     * Log per-date summary
     */
    private void logDateSummary(String date, ProcessingResult result, long timeMs) {
        log.info("─".repeat(70));
        log.info("📊 SUMMARY FOR {}", date);
        log.info("─".repeat(70));
        log.info("   Total Orders: {}", result.getTotalProcessed());
        log.info("   Successfully Processed: {}", result.getSuccessCount());
        log.info("   Failed: {}", result.getFailedCount());
        log.info("   Success Rate: {:.1f}%", result.getSuccessRate());
        log.info("   Processing Time: {}ms", timeMs);

        if (!result.getErrors().isEmpty()) {
            log.warn("   ⚠️ Errors: {}", result.getErrors().size());
        }

        log.info("─".repeat(70));
    }
}