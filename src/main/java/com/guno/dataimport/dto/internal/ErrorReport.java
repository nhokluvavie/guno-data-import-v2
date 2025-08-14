package com.guno.dataimport.dto.internal;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

/**
 * Error details during processing
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ErrorReport {

    @Builder.Default
    private LocalDateTime timestamp = LocalDateTime.now();

    private String entityType;
    private String entityId;
    private String platform;
    private String errorCode;
    private String errorMessage;
    private String stackTrace;

    public static ErrorReport of(String entityType, String entityId, String platform, Exception e) {
        return ErrorReport.builder()
                .entityType(entityType)
                .entityId(entityId)
                .platform(platform)
                .errorCode(e.getClass().getSimpleName())
                .errorMessage(e.getMessage())
                .stackTrace(getStackTrace(e))
                .build();
    }

    private static String getStackTrace(Exception e) {
        return e.getStackTrace().length > 0 ? e.getStackTrace()[0].toString() : "";
    }
}