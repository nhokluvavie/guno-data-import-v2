package com.guno.dataimport.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

/**
 * OrderStatus Entity - Maps to tbl_order_status table
 * UPDATED: Composite primary key changed from (status_key, order_id)
 *          to (status_key, order_id, sub_status_id, partner_status_id)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderStatus {

    // Composite Primary Key (4 fields)
    private Long statusKey;
    private String orderId;
    private String subStatusId;        // NEW - NOT NULL
    private String partnerStatusId;    // NEW - NOT NULL

    @Builder.Default private Integer transitionDateKey = 0;
    private LocalDateTime transitionTimestamp;
    @Builder.Default private Integer durationInPreviousStatusHours = 0;

    private String transitionReason;
    private String transitionTrigger;
    private String changedBy;

    @Builder.Default private Boolean isOnTimeTransition = true;
    @Builder.Default private Boolean isExpectedTransition = true;
    @Builder.Default private Long historyKey = 0L;

    private String createdAt;          // NEW - varchar(255) in schema
}