package com.guno.dataimport.dto.platform.shopee;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * Shopee Order DTO - Top-level wrapper for Shopee order data
 * Maps to root JSON structure from Shopee API response
 * Pattern: Similar to TikTokOrderDto
 *
 * Location: src/main/java/com/guno/dataimport/dto/platform/shopee/ShopeeOrderDto.java
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ShopeeOrderDto {

    @JsonProperty("id")
    private String id;

    @JsonProperty("nhanh_app_id")
    private String nhanhAppId;

    @JsonProperty("order_id")
    private String orderId;

    @JsonProperty("tracking_number")
    private String trackingNumber;

    @JsonProperty("shop_id")
    private Integer shopId;

    @JsonProperty("status")
    private Integer status;

    @JsonProperty("shopee_data")
    private ShopeeData shopeeData;

    @JsonProperty("nhanh_order_id")
    private String nhanhOrderId;

    @JsonProperty("source")
    private String source;

    @JsonProperty("last_synced")
    private String lastSynced;

    @JsonProperty("inserted_at")
    private String insertedAt;

    @JsonProperty("updated_at")
    private String updatedAt;

    @JsonProperty("total_price_after_sub_discount")
    private Long totalPriceAfterSubDiscount;

    @JsonProperty("ads_source")
    private String adsSource;

    @JsonProperty("is_livestream")
    private Integer isLivestream;

    @JsonProperty("assigned_user_id")
    private String assignedUserId;

    @JsonProperty("creator_id")
    private String creatorId;

    @JsonProperty("total_skus")
    private Integer totalSkus;

    @JsonProperty("count_skus")
    private Integer countSkus;

    @JsonProperty("createdAt")
    private String createdAt;

    @JsonProperty("updatedAt")
    private String updatedAt2;

    /**
     * Shopee Data - Nested structure containing order_detail and order_return
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ShopeeData {

        @JsonProperty("order_detail")
        private ShopeeOrderDetail orderDetail;

        @JsonProperty("order_return")
        private ShopeeOrderReturn orderReturn;
    }

    // ========== HELPER METHODS ==========

    public ShopeeOrderDetail getOrderDetail() {
        return shopeeData != null ? shopeeData.getOrderDetail() : null;
    }

    public ShopeeOrderReturn getOrderReturn() {
        return shopeeData != null ? shopeeData.getOrderReturn() : null;
    }

    public boolean hasShopeeData() {
        return shopeeData != null;
    }

    public boolean hasOrderDetail() {
        return getOrderDetail() != null;
    }

    public boolean hasOrderReturn() {
        return getOrderReturn() != null && getOrderReturn().getStatus() != null;
    }

    public String getOrderIdSafe() {
        if (hasOrderDetail()) {
            return getOrderDetail().getOrderSn();
        }
        return orderId;
    }

    public String getShopeeOrderStatus() {
        return hasOrderDetail() ? getOrderDetail().getOrderStatus() : null;
    }

    public String getShopeeReturnStatus() {
        return hasOrderReturn() ? getOrderReturn().getStatus() : null;
    }

    public LocalDateTime getInsertedAt() {
        if (insertedAt == null || insertedAt.trim().isEmpty()) return null;
        try {
            return java.time.ZonedDateTime.parse(insertedAt, java.time.format.DateTimeFormatter.ISO_DATE_TIME)
                    .withZoneSameInstant(ZoneId.of("Asia/Ho_Chi_Minh")).toLocalDateTime();
        } catch (Exception e) {
            return null;
        }
    }

    public String getTrackingNumberSafe() {
        if (hasOrderDetail() && getOrderDetail().getPackageList() != null
                && !getOrderDetail().getPackageList().isEmpty()) {
            return getOrderDetail().getPackageList().get(0).getPackageNumber();
        }
        return trackingNumber;
    }

    public Integer getStatusSafe() {
        return status;
    }

    public boolean isLivestreamOrder() {
        return isLivestream != null && isLivestream == 1;
    }

    public boolean isCodOrder() {
        return hasOrderDetail() && Boolean.TRUE.equals(getOrderDetail().getCod());
    }
}