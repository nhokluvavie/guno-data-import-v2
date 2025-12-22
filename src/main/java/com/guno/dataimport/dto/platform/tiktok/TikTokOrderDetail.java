package com.guno.dataimport.dto.platform.tiktok;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * TikTok Order Detail DTO - Main order details from TikTok API
 * Maps to "order_detail" object in TikTok order JSON
 * This is the core wrapper containing all order information
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TikTokOrderDetail {

    @JsonProperty("id")
    private String id;

    @JsonProperty("status")
    private String status;

    @JsonProperty("order_type")
    private String orderType;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("buyer_email")
    private String buyerEmail;

    @JsonProperty("buyer_message")
    private String buyerMessage;

    @JsonProperty("is_cod")
    private Boolean isCod;

    @JsonProperty("create_time")
    private Long createTime;

    @JsonProperty("update_time")
    private Long updateTime;

    @JsonProperty("paid_time")
    private Long paidTime;

    @JsonProperty("delivery_time")
    private Long deliveryTime;

    @JsonProperty("collection_time")
    private Long collectionTime;

    @JsonProperty("tracking_number")
    private String trackingNumber;

    @JsonProperty("shipping_provider")
    private String shippingProvider;

    @JsonProperty("shipping_provider_id")
    private String shippingProviderId;

    @JsonProperty("shipping_type")
    private String shippingType;

    @JsonProperty("delivery_type")
    private String deliveryType;

    @JsonProperty("delivery_option_id")
    private String deliveryOptionId;

    @JsonProperty("delivery_option_name")
    private String deliveryOptionName;

    @JsonProperty("fulfillment_type")
    private String fulfillmentType;

    @JsonProperty("warehouse_id")
    private String warehouseId;

    @JsonProperty("commerce_platform")
    private String commercePlatform;

    @JsonProperty("payment_method_code")
    private String paymentMethodCode;

    @JsonProperty("payment_method_name")
    private String paymentMethodName;

    @JsonProperty("cancel_reason")
    private String cancelReason;

    @JsonProperty("cancellation_initiator")
    private String cancellationInitiator;

    @JsonProperty("is_sample_order")
    private Boolean isSampleOrder;

    @JsonProperty("is_on_hold_order")
    private Boolean isOnHoldOrder;

    @JsonProperty("is_replacement_order")
    private Boolean isReplacementOrder;

    @JsonProperty("has_updated_recipient_address")
    private Boolean hasUpdatedRecipientAddress;

    @JsonProperty("rts_time")
    private Long rtsTime;

    @JsonProperty("rts_sla_time")
    private Long rtsSlaTime;

    @JsonProperty("tts_sla_time")
    private Long ttsSlaTime;

    @JsonProperty("delivery_sla_time")
    private Long deliverySlaTime;

    @JsonProperty("shipping_due_time")
    private Long shippingDueTime;

    @JsonProperty("collection_due_time")
    private Long collectionDueTime;

    @JsonProperty("cancel_order_sla_time")
    private Long cancelOrderSlaTime;

    @JsonProperty("recommended_shipping_time")
    private Long recommendedShippingTime;

    @JsonProperty("fulfillment_priority_level")
    private Integer fulfillmentPriorityLevel;

    @JsonProperty("payment")
    private TikTokPayment payment;

    @JsonProperty("recipient_address")
    private TikTokRecipientAddress recipientAddress;

    @JsonProperty("line_items")
    @Builder.Default
    private List<TikTokLineItem> lineItems = new ArrayList<>();

    @JsonProperty("packages")
    @Builder.Default
    private List<TikTokPackage> packages = new ArrayList<>();

    @JsonProperty("cancel_time")
    private Long cancelTime;

    // Helper methods for easier data access

    public String getOrderId() {
        return id;
    }

    public boolean isCashOnDelivery() {
        return isCod != null && isCod;
    }

    public boolean hasLineItems() {
        return lineItems != null && !lineItems.isEmpty();
    }

    public int getLineItemCount() {
        return lineItems != null ? lineItems.size() : 0;
    }

    public boolean hasPackages() {
        return packages != null && !packages.isEmpty();
    }

    public String getFirstPackageId() {
        if (hasPackages()) {
            return packages.get(0).getPackageId();
        }
        return null;
    }

    public boolean isDelivered() {
        return "DELIVERED".equals(status);
    }

    public boolean isCancelled() {
        return cancelReason != null || cancellationInitiator != null;
    }

    public String getCustomerName() {
        return recipientAddress != null ? recipientAddress.getFullName() : null;
    }

    public String getCustomerPhone() {
        return recipientAddress != null ? recipientAddress.getPhoneNumber() : null;
    }

    public String getProvince() {
        return recipientAddress != null ? recipientAddress.getProvince() : null;
    }

    public String getDistrict() {
        return recipientAddress != null ? recipientAddress.getDistrict() : null;
    }

    public String getWard() {
        return recipientAddress != null ? recipientAddress.getWard() : null;
    }

    public Double getTotalAmount() {
        return payment != null ? payment.getTotalAmountAsDouble() : 0.0;
    }

    public Double getShippingFee() {
        return payment != null ? payment.getShippingFeeAsDouble() : 0.0;
    }

    public Double getTotalDiscount() {
        return payment != null ? payment.getTotalDiscountAsDouble() : 0.0;
    }
}