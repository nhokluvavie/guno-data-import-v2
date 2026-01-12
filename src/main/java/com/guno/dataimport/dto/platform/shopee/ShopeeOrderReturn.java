package com.guno.dataimport.dto.platform.shopee;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ShopeeOrderReturn - Return/refund details from Shopee
 * Location: src/main/java/com/guno/dataimport/dto/platform/shopee/ShopeeOrderReturn.java
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ShopeeOrderReturn {

    @JsonProperty("return_sn")
    private String returnSn;

    @JsonProperty("order_sn")
    private String orderSn;

    @JsonProperty("status")
    private String status;

    @JsonProperty("reason")
    private String reason;

    @JsonProperty("text_reason")
    private String textReason;

    @JsonProperty("refund_amount")
    private Long refundAmount;

    @JsonProperty("amount_before_discount")
    private Long amountBeforeDiscount;

    @JsonProperty("return_solution")
    private Integer returnSolution;

    @JsonProperty("return_refund_type")
    private String returnRefundType;

    @JsonProperty("tracking_number")
    private String trackingNumber;

    @JsonProperty("create_time")
    private Long createTime;

    @JsonProperty("update_time")
    private Long updateTime;

    @JsonProperty("due_date")
    private Long dueDate;

    @JsonProperty("return_ship_due_date")
    private Long returnShipDueDate;

    @JsonProperty("return_seller_due_date")
    private Long returnSellerDueDate;

    @JsonProperty("needs_logistics")
    private Boolean needsLogistics;

    @JsonProperty("is_seller_arrange")
    private Boolean isSellerArrange;

    @JsonProperty("is_arrived_at_warehouse")
    private Integer isArrivedAtWarehouse;

    @JsonProperty("negotiation_status")
    private String negotiationStatus;

    @JsonProperty("seller_proof_status")
    private String sellerProofStatus;

    @JsonProperty("currency")
    private String currency;

    @JsonProperty("validation_type")
    private String validationType;

    @JsonProperty("hot_listing_order")
    private Boolean hotListingOrder;

    @JsonProperty("seller_compensation_status")
    private String sellerCompensationStatus;

    @JsonProperty("is_shipping_proof_mandatory")
    private Boolean isShippingProofMandatory;

    @JsonProperty("reassessed_request_reason")
    private String reassessedRequestReason;

    @JsonProperty("return_refund_request_type")
    private Integer returnRefundRequestType;

    // Helper methods
    public Double getRefundAmountAsDouble() {
        return refundAmount != null ? refundAmount.doubleValue() : 0.0;
    }

    public boolean isProcessing() {
        return "PROCESSING".equalsIgnoreCase(status);
    }

    public boolean isCompleted() {
        return "COMPLETED".equalsIgnoreCase(status);
    }
}