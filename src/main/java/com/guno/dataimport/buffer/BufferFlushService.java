package com.guno.dataimport.buffer;

import com.guno.dataimport.dto.internal.CollectedData;
import com.guno.dataimport.dto.internal.ProcessingResult;
import com.guno.dataimport.processor.BatchProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Buffer Flush Service - Coordinate bulk processing of buffered data
 * Handles 11-table flush operations efficiently
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class BufferFlushService {

    private final BatchProcessor batchProcessor;

    /**
     * Flush buffer to database (11 tables in 1 transaction)
     */
    public ProcessingResult flush(MemoryBuffer buffer) {
        if (buffer.isEmpty()) {
            log.debug("Buffer empty, nothing to flush");
            return ProcessingResult.builder().build();
        }

        log.info("Flushing buffer: {} orders → 11 tables", buffer.size());

        long startTime = System.currentTimeMillis();

        // Convert buffer to CollectedData
        CollectedData collectedData = new CollectedData();
        collectedData.setFacebookOrders(buffer.getOrders().stream()
                .map(order -> (Object) order)
                .toList());

        // Single bulk processing for all 11 tables
        ProcessingResult result = batchProcessor.processCollectedData(collectedData);

        long duration = System.currentTimeMillis() - startTime;

        log.info("Buffer flushed: {} orders, {} success, {}ms",
                buffer.size(), result.getSuccessCount(), duration);

        return result;
    }

    /**
     * Auto-flush if buffer is full
     */
    public ProcessingResult autoFlush(MemoryBuffer buffer) {
        if (buffer.isFull()) {
            ProcessingResult result = flush(buffer);
            buffer.clear();
            return result;
        }
        return ProcessingResult.builder().build();
    }

    /**
     * Force flush remaining data
     */
    public ProcessingResult forceFlush(MemoryBuffer buffer) {
        if (!buffer.isEmpty()) {
            ProcessingResult result = flush(buffer);
            buffer.clear();
            return result;
        }
        return ProcessingResult.builder().build();
    }
}