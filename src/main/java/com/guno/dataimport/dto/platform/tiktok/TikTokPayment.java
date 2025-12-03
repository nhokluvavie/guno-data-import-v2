package com.guno.dataimport.dto.platform.tiktok;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * TikTok Payment DTO - Payment information from TikTok API
 * Maps to "payment" object in TikTok order JSON
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TikTokPayment {

    @JsonProperty("currency")
    private String currency;

    @JsonProperty("sub_total")
    private String subTotal;

    @JsonProperty("total_amount")
    private String totalAmount;

    @JsonProperty("tax")
    private String tax;

    @JsonProperty("shipping_fee")
    private String shippingFee;

    @JsonProperty("original_shipping_fee")
    private String originalShippingFee;

    @JsonProperty("seller_discount")
    private String sellerDiscount;

    @JsonProperty("platform_discount")
    private String platformDiscount;

    @JsonProperty("original_total_product_price")
    private String originalTotalProductPrice;

    @JsonProperty("shipping_fee_seller_discount")
    private String shippingFeeSellerDiscount;

    @JsonProperty("shipping_fee_platform_discount")
    private String shippingFeePlatformDiscount;

    @JsonProperty("shipping_fee_cofunded_discount")
    private String shippingFeeCofundedDiscount;

    // Helper methods for easier data access and conversion

    public Double getTotalAmountAsDouble() {
        return parseAmount(totalAmount);
    }

    public Double getSubTotalAsDouble() {
        return parseAmount(subTotal);
    }

    public Double getShippingFeeAsDouble() {
        return parseAmount(shippingFee);
    }

    public Double getOriginalShippingFeeAsDouble() {
        return parseAmount(originalShippingFee);
    }

    public Double getTaxAsDouble() {
        return parseAmount(tax);
    }

    public Double getSellerDiscountAsDouble() {
        return parseAmount(sellerDiscount);
    }

    public Double getPlatformDiscountAsDouble() {
        return parseAmount(platformDiscount);
    }

    public Double getTotalDiscountAsDouble() {
        return getSellerDiscountAsDouble() + getPlatformDiscountAsDouble();
    }

    public Double getOriginalTotalProductPriceAsDouble() {
        return parseAmount(originalTotalProductPrice);
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