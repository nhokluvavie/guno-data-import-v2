package com.guno.dataimport.dto.platform.tiktok;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * TikTok Line Item DTO - Order item details from TikTok API
 * Maps to "line_items" array in TikTok order JSON
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TikTokLineItem {

    @JsonProperty("id")
    private String id;

    @JsonProperty("sku_id")
    private String skuId;

    @JsonProperty("product_id")
    private String productId;

    @JsonProperty("seller_sku")
    private String sellerSku;

    @JsonProperty("product_name")
    private String productName;

    @JsonProperty("sku_name")
    private String skuName;

    @JsonProperty("sku_type")
    private String skuType;

    @JsonProperty("sku_image")
    private String skuImage;

    @JsonProperty("original_price")
    private String originalPrice;

    @JsonProperty("sale_price")
    private String salePrice;

    @JsonProperty("seller_discount")
    private String sellerDiscount;

    @JsonProperty("platform_discount")
    private String platformDiscount;

    @JsonProperty("currency")
    private String currency;

    @JsonProperty("is_gift")
    private Boolean isGift;

    @JsonProperty("gift_retail_price")
    private String giftRetailPrice;

    @JsonProperty("package_id")
    private String packageId;

    @JsonProperty("package_status")
    private String packageStatus;

    @JsonProperty("display_status")
    private String displayStatus;

    @JsonProperty("tracking_number")
    private String trackingNumber;

    @JsonProperty("shipping_provider_id")
    private String shippingProviderId;

    @JsonProperty("shipping_provider_name")
    private String shippingProviderName;

    @JsonProperty("cancel_user")
    private String cancelUser;

    @JsonProperty("cancel_reason")
    private String cancelReason;

    @JsonProperty("rts_time")
    private Long rtsTime;

    // Helper methods for easier data access

    public String getSku() {
        return sellerSku != null ? sellerSku : skuId;
    }

    public Double getSalePriceAsDouble() {
        if (salePrice == null || salePrice.isEmpty()) return 0.0;
        try {
            return Double.parseDouble(salePrice);
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    public Double getOriginalPriceAsDouble() {
        if (originalPrice == null || originalPrice.isEmpty()) return 0.0;
        try {
            return Double.parseDouble(originalPrice);
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    public boolean isGiftItem() {
        return isGift != null && isGift;
    }

    public boolean isCancelled() {
        return cancelUser != null || cancelReason != null;
    }
}