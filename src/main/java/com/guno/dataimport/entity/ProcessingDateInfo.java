package com.guno.dataimport.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

/**
 * ProcessingDateInfo Entity - Maps to tbl_processing_date_info table
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessingDateInfo {

    private String orderId;
    private Long dateKey;
    private LocalDateTime fullDate;

    // Calendar information
    @Builder.Default private Integer dayOfWeek = 0;
    private String dayOfWeekName;
    @Builder.Default private Integer dayOfMonth = 0;
    @Builder.Default private Integer dayOfYear = 0;
    @Builder.Default private Integer weekOfYear = 0;
    @Builder.Default private Integer monthOfYear = 0;
    private String monthName;
    @Builder.Default private Integer quarterOfYear = 0;
    private String quarterName;
    @Builder.Default private Integer year = 0;

    // Business calendar
    @Builder.Default private Boolean isWeekend = false;
    @Builder.Default private Boolean isHoliday = false;
    private String holidayName;
    @Builder.Default private Boolean isBusinessDay = true;

    // Fiscal calendar
    @Builder.Default private Integer fiscalYear = 0;
    @Builder.Default private Integer fiscalQuarter = 0;

    // Business seasons
    @Builder.Default private Boolean isShoppingSeason = false;
    private String seasonName;
    @Builder.Default private Boolean isPeakHour = false;
}