package com.guno.dataimport.dto.internal;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Summary of entire import process
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ImportSummary {

    @Builder.Default
    private LocalDateTime startTime = LocalDateTime.now();

    private LocalDateTime endTime;

    // Platform-specific results
    @Builder.Default
    private Map<String, Integer> platformCounts = new HashMap<>();

    // Entity-specific results
    @Builder.Default
    private Map<String, Integer> entityCounts = new HashMap<>();

    @Builder.Default
    private Integer totalApiCalls = 0;

    @Builder.Default
    private Integer totalDbOperations = 0;

    @Builder.Default
    private Boolean parallelMode = false;

    public long getDurationMs() {
        return endTime != null ?
                java.time.Duration.between(startTime, endTime).toMillis() : 0;
    }

    public String getDurationFormatted() {
        long ms = getDurationMs();
        long minutes = ms / 60000;
        long seconds = (ms % 60000) / 1000;
        return String.format("%d:%02d", minutes, seconds);
    }

    public void addPlatformCount(String platform, int count) {
        platformCounts.put(platform, count);
    }

    public void addEntityCount(String entity, int count) {
        entityCounts.put(entity, count);
    }
}