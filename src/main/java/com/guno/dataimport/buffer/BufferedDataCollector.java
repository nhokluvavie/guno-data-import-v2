package com.guno.dataimport.buffer;

import com.guno.dataimport.api.client.FacebookApiClient;
import com.guno.dataimport.dto.internal.ImportSummary;
import com.guno.dataimport.dto.internal.ProcessingResult;
import com.guno.dataimport.dto.platform.facebook.FacebookApiResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.time.LocalDateTime;

/**
 * Buffered Data Collector - High-performance collection with memory buffering
 * Reduces DB operations from N*11 to (N/bufferSize)*11
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class BufferedDataCollector {

    private final FacebookApiClient facebookApiClient;
    private final BufferFlushService bufferFlushService;

    /**
     * Collect with buffering strategy - PERFORMANCE OPTIMIZED
     */
    public ImportSummary collectWithBuffer(int bufferSize, int pageSize) {
        log.info("Starting buffered collection - Buffer: {}, PageSize: {}", bufferSize, pageSize);

        ImportSummary summary = ImportSummary.builder()
                .startTime(LocalDateTime.now())
                .parallelMode(false)
                .build();

        MemoryBuffer buffer = new MemoryBuffer(bufferSize);
        int currentPage = 1;
        int totalProcessed = 0;
        int totalApiCalls = 0;
        int totalFlushes = 0;

        try {
            boolean hasMoreData = true;

            while (hasMoreData) {
                // API call
                FacebookApiResponse response = facebookApiClient.fetchOrders("", currentPage, pageSize);
                totalApiCalls++;

                if (response.getData() != null && !response.getData().getOrders().isEmpty()) {
                    // Add to buffer
                    buffer.addOrders(response.getData().getOrders());
                    log.debug("Page {} → Buffer: {}/{}", currentPage, buffer.size(), buffer.getCapacity());

                    // Auto-flush if full
                    ProcessingResult flushResult = bufferFlushService.autoFlush(buffer);
                    if (flushResult.getTotalProcessed() > 0) {
                        totalProcessed += flushResult.getSuccessCount();
                        totalFlushes++;
                        log.info("Auto-flush #{}: {} orders processed", totalFlushes, flushResult.getSuccessCount());
                    }

                    // Check continuation
                    hasMoreData = response.getData().getOrders().size() >= pageSize;
                    currentPage++;
                    Thread.sleep(500); // Reduced delay for buffered approach

                } else {
                    hasMoreData = false;
                }
            }

            // Force flush remaining
            ProcessingResult finalFlush = bufferFlushService.forceFlush(buffer);
            if (finalFlush.getTotalProcessed() > 0) {
                totalProcessed += finalFlush.getSuccessCount();
                totalFlushes++;
                log.info("Final flush: {} orders processed", finalFlush.getSuccessCount());
            }

        } catch (Exception e) {
            log.error("Buffered collection error: {}", e.getMessage(), e);
        }

        // Update summary
        summary.addPlatformCount("FACEBOOK", totalProcessed);
        summary.setTotalApiCalls(totalApiCalls);
        summary.setTotalDbOperations(totalFlushes * 11); // 11 tables per flush
        summary.setEndTime(LocalDateTime.now());

        log.info("Buffered collection completed - Processed: {}, API calls: {}, DB flushes: {}, Duration: {}",
                totalProcessed, totalApiCalls, totalFlushes, summary.getDurationFormatted());

        return summary;
    }

    /**
     * Performance comparison method
     */
    public void logPerformanceGains(int totalOrders, int bufferSize, int pageSize) {
        int pages = (int) Math.ceil((double) totalOrders / pageSize);
        int oldDbOps = pages * 11; // Old: 11 DB ops per page
        int newDbOps = (int) Math.ceil((double) totalOrders / bufferSize) * 11; // New: 11 DB ops per buffer flush

        double improvement = ((double) oldDbOps - newDbOps) / oldDbOps * 100;

        log.info("PERFORMANCE ANALYSIS:");
        log.info("  Orders: {}, Buffer: {}, PageSize: {}", totalOrders, bufferSize, pageSize);
        log.info("  Old approach: {} DB operations", oldDbOps);
        log.info("  New approach: {} DB operations", newDbOps);
        log.info("  Improvement: {:.1f}% reduction", improvement);
    }
}