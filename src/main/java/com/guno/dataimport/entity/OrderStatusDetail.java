package com.guno.dataimport.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * OrderStatusDetail Entity - Maps to tbl_order_status_detail table
 * Uses composite primary key (statusKey, orderId)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderStatusDetail {

    // Composite Primary Key
    private Long statusKey;
    private String orderId;

    // Order state flags
    @Builder.Default private Boolean isActiveOrder = true;
    @Builder.Default private Boolean isCompletedOrder = false;
    @Builder.Default private Boolean isRevenueRecognized = false;
    @Builder.Default private Boolean isRefundable = false;
    @Builder.Default private Boolean isCancellable = true;
    @Builder.Default private Boolean isTrackable = false;

    private String nextPossibleStatuses;
    @Builder.Default private Integer autoTransitionHours = 0;
    @Builder.Default private Boolean requiresManualAction = false;

    // UI presentation
    private String statusColor;
    private String statusIcon;
    @Builder.Default private Boolean customerVisible = true;
    private String customerDescription;

    // Analytics
    @Builder.Default private Double averageDurationHours = 0.0;
    @Builder.Default private Double successRate = 100.0;
}
