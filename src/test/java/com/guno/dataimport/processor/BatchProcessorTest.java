package com.guno.dataimport.processor;

import com.guno.dataimport.DataImportApplication;
import com.guno.dataimport.api.client.FacebookApiClient;
import com.guno.dataimport.dto.internal.CollectedData;
import com.guno.dataimport.dto.internal.ProcessingResult;
import com.guno.dataimport.dto.platform.facebook.FacebookApiResponse;
import com.guno.dataimport.repository.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import static org.assertj.core.api.Assertions.*;

/**
 * BatchProcessor Test - Test complete data processing pipeline
 * Location: src/test/java/com/guno/dataimport/processor/BatchProcessorTest.java
 */
@SpringBootTest(classes = DataImportApplication.class)
@ActiveProfiles("test")
@Slf4j
class BatchProcessorTest {

    @Autowired private BatchProcessor batchProcessor;
    @Autowired private FacebookApiClient facebookApiClient;

    @Autowired private CustomerRepository customerRepository;
    @Autowired private OrderRepository orderRepository;
    @Autowired private OrderItemRepository orderItemRepository;
    @Autowired private ProductRepository productRepository;

    @Test
    @Transactional
    void testCompleteProcessingPipeline_WithRealData() {
        log.info("=== BatchProcessor End-to-End Test ===");

        // Step 1: Get real data
        FacebookApiResponse apiResponse = facebookApiClient.fetchOrders();
        if (apiResponse.getData() == null || apiResponse.getData().getOrders().isEmpty()) {
            log.warn("No API data available - skipping test");
            return;
        }

        // Step 2: Create CollectedData
        CollectedData collectedData = new CollectedData();
        collectedData.setFacebookOrders(apiResponse.getData().getOrders().stream()
                .map(order -> (Object) order)
                .toList());

        log.info("Processing {} orders", collectedData.getFacebookOrders().size());

        // Step 3: Process data
        ProcessingResult result = batchProcessor.processCollectedData(collectedData);

        // Step 4: Verify results
        assertThat(result).isNotNull();
        assertThat(result.getTotalProcessed()).isEqualTo(collectedData.getFacebookOrders().size());
        assertThat(result.getSuccessCount()).isGreaterThan(0);
        assertThat(result.getProcessingTimeMs()).isGreaterThan(0);

        log.info("Processing completed - Success: {}, Failed: {}, Duration: {}ms",
                result.getSuccessCount(), result.getFailedCount(), result.getProcessingTimeMs());

        // Step 5: Verify database records
        verifyDatabaseRecords(result.getSuccessCount());

        log.info("=== BatchProcessor test completed successfully ===");
    }

    @Test
    void testProcessing_WithEmptyData() {
        log.info("Testing BatchProcessor with empty data");

        CollectedData emptyData = new CollectedData();
        ProcessingResult result = batchProcessor.processCollectedData(emptyData);

        assertThat(result).isNotNull();
        assertThat(result.getTotalProcessed()).isEqualTo(0);
        assertThat(result.getSuccessCount()).isEqualTo(0);
        assertThat(result.getFailedCount()).isEqualTo(0);

        log.info("Empty data test passed");
    }

    @Test
    void testProcessing_WithNullData() {
        log.info("Testing BatchProcessor with null data");

        ProcessingResult result = batchProcessor.processCollectedData(null);

        assertThat(result).isNotNull();
        assertThat(result.getTotalProcessed()).isEqualTo(0);
        assertThat(result.getErrors()).isNotEmpty(); // Should have error for null data

        log.info("Null data test passed");
    }

    @Test
    @Transactional
    void testProcessing_PerformanceMetrics() {
        log.info("Testing BatchProcessor performance");

        // Get small batch for performance test
        FacebookApiResponse apiResponse = facebookApiClient.fetchOrders("", 1, 5); // Small batch
        if (apiResponse.getData() == null || apiResponse.getData().getOrders().isEmpty()) {
            log.warn("No API data - skipping performance test");
            return;
        }

        CollectedData data = new CollectedData();
        data.setFacebookOrders(apiResponse.getData().getOrders().stream()
                .map(order -> (Object) order)
                .toList());

        long startTime = System.currentTimeMillis();
        ProcessingResult result = batchProcessor.processCollectedData(data);
        long duration = System.currentTimeMillis() - startTime;

        // Performance assertions
        assertThat(result.getProcessingTimeMs()).isLessThanOrEqualTo(duration + 100); // Allow small variance
        assertThat(result.getProcessingTimeMs()).isGreaterThan(0);
        assertThat(result.getSuccessRate()).isGreaterThan(80.0); // At least 80% success rate

        log.info("Performance test - Duration: {}ms, Success Rate: {}%",
                duration, result.getSuccessRate());
    }

    private void verifyDatabaseRecords(int expectedOrders) {
        // Verify customers were saved
        long customerCount = customerRepository.count();
        assertThat(customerCount).isGreaterThan(0);
        log.info("Verified {} customers in database", customerCount);

        // Verify orders were saved
        long orderCount = orderRepository.count();
        assertThat(orderCount).isGreaterThanOrEqualTo(expectedOrders);
        log.info("Verified {} orders in database", orderCount);

        // Verify order items were saved
        long itemCount = orderItemRepository.count();
        assertThat(itemCount).isGreaterThan(0);
        log.info("Verified {} order items in database", itemCount);

        // Verify products were saved
        long productCount = productRepository.count();
        assertThat(productCount).isGreaterThan(0);
        log.info("Verified {} products in database", productCount);
    }
}