package com.guno.dataimport.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

/**
 * OrderStatus Entity - Maps to tbl_order_status table
 * Uses composite primary key (statusKey, orderId)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderStatus {

    // Composite Primary Key
    private Long statusKey;
    private String orderId;

    @Builder.Default private Integer transitionDateKey = 0;
    private LocalDateTime transitionTimestamp;
    @Builder.Default private Integer durationInPreviousStatusHours = 0;

    private String transitionReason;
    private String transitionTrigger;
    private String changedBy;

    @Builder.Default private Boolean isOnTimeTransition = true;
    @Builder.Default private Boolean isExpectedTransition = true;
    @Builder.Default private Long historyKey = 0L;
}
