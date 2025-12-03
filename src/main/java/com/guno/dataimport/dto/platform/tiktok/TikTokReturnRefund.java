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
 * TikTok Return Refund DTO - Return and refund information from TikTok API
 * Maps to "return_refund" object in TikTok order JSON
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TikTokReturnRefund {

    @JsonProperty("return_id")
    private String returnId;

    @JsonProperty("order_id")
    private String orderId;

    @JsonProperty("role")
    private String role;

    @JsonProperty("return_type")
    private String returnType;

    @JsonProperty("return_status")
    private String returnStatus;

    @JsonProperty("return_reason")
    private String returnReason;

    @JsonProperty("return_reason_text")
    private String returnReasonText;

    @JsonProperty("return_method")
    private String returnMethod;

    @JsonProperty("handover_method")
    private String handoverMethod;

    @JsonProperty("shipment_type")
    private String shipmentType;

    @JsonProperty("create_time")
    private Long createTime;

    @JsonProperty("update_time")
    private Long updateTime;

    @JsonProperty("refund_amount")
    private RefundAmount refundAmount;

    @JsonProperty("return_line_items")
    @Builder.Default
    private List<ReturnLineItem> returnLineItems = new ArrayList<>();

    @JsonProperty("discount_amount")
    @Builder.Default
    private List<DiscountAmount> discountAmount = new ArrayList<>();

    @JsonProperty("shipping_fee_amount")
    @Builder.Default
    private List<ShippingFeeAmount> shippingFeeAmount = new ArrayList<>();

    @JsonProperty("return_provider_id")
    private String returnProviderId;

    @JsonProperty("return_provider_name")
    private String returnProviderName;

    @JsonProperty("combined_return_id")
    private String combinedReturnId;

    @JsonProperty("is_combined_return")
    private Boolean isCombinedReturn;

    @JsonProperty("return_warehouse_address")
    private ReturnWarehouseAddress returnWarehouseAddress;

    /**
     * Refund Amount - Nested structure
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RefundAmount {

        @JsonProperty("currency")
        private String currency;

        @JsonProperty("refund_total")
        private String refundTotal;

        @JsonProperty("refund_subtotal")
        private String refundSubtotal;

        @JsonProperty("refund_shipping_fee")
        private String refundShippingFee;

        @JsonProperty("refund_tax")
        private String refundTax;

        public Double getRefundTotalAsDouble() {
            return parseAmount(refundTotal);
        }

        public Double getRefundSubtotalAsDouble() {
            return parseAmount(refundSubtotal);
        }

        private Double parseAmount(String amount) {
            if (amount == null || amount.isEmpty()) return 0.0;
            try {
                return Double.parseDouble(amount);
            } catch (NumberFormatException e) {
                return 0.0;
            }
        }
    }

    /**
     * Return Line Item - Nested structure
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ReturnLineItem {

        @JsonProperty("return_line_item_id")
        private String returnLineItemId;

        @JsonProperty("order_line_item_id")
        private String orderLineItemId;

        @JsonProperty("sku_id")
        private String skuId;

        @JsonProperty("seller_sku")
        private String sellerSku;

        @JsonProperty("product_name")
        private String productName;

        @JsonProperty("sku_name")
        private String skuName;

        @JsonProperty("product_image")
        private ProductImage productImage;

        @JsonProperty("refund_amount")
        private RefundAmount refundAmount;
    }

    /**
     * Product Image - Nested structure
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ProductImage {

        @JsonProperty("url")
        private String url;

        @JsonProperty("width")
        private Integer width;

        @JsonProperty("height")
        private Integer height;
    }

    /**
     * Discount Amount - Nested structure
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DiscountAmount {

        @JsonProperty("currency")
        private String currency;

        @JsonProperty("product_seller_discount")
        private String productSellerDiscount;

        @JsonProperty("product_platform_discount")
        private String productPlatformDiscount;

        @JsonProperty("shipping_fee_seller_discount")
        private String shippingFeeSellerDiscount;

        @JsonProperty("shipping_fee_platform_discount")
        private String shippingFeePlatformDiscount;
    }

    /**
     * Shipping Fee Amount - Nested structure
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ShippingFeeAmount {

        @JsonProperty("currency")
        private String currency;

        @JsonProperty("buyer_paid_return_shipping_fee")
        private String buyerPaidReturnShippingFee;

        @JsonProperty("seller_paid_return_shipping_fee")
        private String sellerPaidReturnShippingFee;

        @JsonProperty("platform_paid_return_shipping_fee")
        private String platformPaidReturnShippingFee;
    }

    /**
     * Return Warehouse Address - Nested structure
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ReturnWarehouseAddress {

        @JsonProperty("full_address")
        private String fullAddress;
    }

    // Helper methods for easier data access

    public boolean hasReturn() {
        return returnId != null && !returnId.isEmpty();
    }

    public boolean isRefundOnly() {
        return "REFUND_ONLY".equals(returnType);
    }

    public boolean isReturnAndRefund() {
        return "RETURN_AND_REFUND".equals(returnType);
    }

    public Double getTotalRefundAmount() {
        return refundAmount != null ? refundAmount.getRefundTotalAsDouble() : 0.0;
    }

    public int getReturnItemCount() {
        return returnLineItems != null ? returnLineItems.size() : 0;
    }
}