package com.guno.dataimport.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

/**
 * Order Entity - Maps to tbl_order table
 * UPDATED: Added 5 new fields for refund/return/exchange handling
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Order {

    private String orderId;
    private String customerId;
    private String shopId;
    private String internalUuid;

    @Builder.Default private Integer orderCount = 1;
    @Builder.Default private Integer itemQuantity = 0;
    @Builder.Default private Integer totalItemsInOrder = 0;

    @Builder.Default private Double grossRevenue = 0.0;
    @Builder.Default private Double netRevenue = 0.0;
    @Builder.Default private Double shippingFee = 0.0;
    @Builder.Default private Double taxAmount = 0.0;
    @Builder.Default private Double discountAmount = 0.0;
    @Builder.Default private Double codAmount = 0.0;
    @Builder.Default private Double platformFee = 0.0;
    @Builder.Default private Double sellerDiscount = 0.0;
    @Builder.Default private Double platformDiscount = 0.0;
    @Builder.Default private Double originalPrice = 0.0;
    @Builder.Default private Double estimatedShippingFee = 0.0;
    @Builder.Default private Double actualShippingFee = 0.0;

    @Builder.Default private Integer shippingWeightGram = 0;
    @Builder.Default private Integer daysToShip = 0;

    @Builder.Default private Boolean isDelivered = false;
    @Builder.Default private Boolean isCancelled = false;
    @Builder.Default private Boolean isReturned = false;
    @Builder.Default private Boolean isCod = false;
    @Builder.Default private Boolean isNewCustomer = false;
    @Builder.Default private Boolean isRepeatCustomer = false;
    @Builder.Default private Boolean isBulkOrder = false;
    @Builder.Default private Boolean isPromotionalOrder = false;
    @Builder.Default private Boolean isSameDayDelivery = false;

    @Builder.Default private Integer orderToShipHours = 0;
    @Builder.Default private Integer shipToDeliveryHours = 0;
    @Builder.Default private Integer totalFulfillmentHours = 0;
    @Builder.Default private Integer customerOrderSequence = 1;
    @Builder.Default private Integer customerLifetimeOrders = 1;
    @Builder.Default private Double customerLifetimeValue = 0.0;
    @Builder.Default private Integer daysSinceLastOrder = 0;
    @Builder.Default private Double promotionImpact = 0.0;
    @Builder.Default private Double adRevenue = 0.0;
    @Builder.Default private Double organicRevenue = 0.0;
    @Builder.Default private Double aov = 0.0;
    @Builder.Default private Double shippingCostRatio = 0.0;

    private LocalDateTime createdAt;
    private Integer rawData;
    private Integer platformSpecificData;

    // Seller fields
    private String sellerId;
    private String sellerName;
    private String sellerEmail;
    private Long latestStatus;

    // ================================
    // NEW FIELDS - Refund/Return/Exchange
    // ================================

    /**
     * isRefunded - Indicates if order has been refunded
     * TikTok: from tiktok_data.return_refund.return_type
     * Facebook: NULL (not supported)
     * Shopee: from existing logic
     */
    @Builder.Default private Boolean isRefunded = false;

    /**
     * refundAmount - Total amount refunded
     * TikTok: from tiktok_data.return_refund.refund_amount.refund_total
     * Facebook: NULL
     * Shopee: calculated from items
     */
    @Builder.Default private Double refundAmount = 0.0;

    /**
     * refundDate - Date when refund was processed
     * TikTok: from tiktok_data.return_refund.update_time (converted from Unix timestamp)
     * Facebook: NULL
     * Shopee: from history
     */
    private String refundDate;

    /**
     * isExchanged - Indicates if order is an exchange
     * ALL PLATFORMS: Check if order has tag "GH1P"
     */
    @Builder.Default private Boolean isExchanged = false;

    /**
     * cancelReason - Reason for cancellation
     * ALL PLATFORMS: Extracted from order notes/history when status = 6 (Canceled)
     */
    private String cancelReason;
}