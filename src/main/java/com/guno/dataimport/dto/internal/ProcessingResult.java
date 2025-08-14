package com.guno.dataimport.dto.internal;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Result of batch processing operation
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessingResult {

    @Builder.Default
    private LocalDateTime processedAt = LocalDateTime.now();

    @Builder.Default
    private Integer totalProcessed = 0;

    @Builder.Default
    private Integer successCount = 0;

    @Builder.Default
    private Integer failedCount = 0;

    @Builder.Default
    private Long processingTimeMs = 0L;

    @Builder.Default
    private List<ErrorReport> errors = new ArrayList<>();

    public double getSuccessRate() {
        return totalProcessed > 0 ? (double) successCount / totalProcessed * 100 : 0;
    }

    public boolean isSuccess() {
        return failedCount == 0;
    }
}