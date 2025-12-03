package com.guno.dataimport.dto.platform.tiktok;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * TikTok Order DTO - Top-level wrapper for TikTok order data
 * Maps to root JSON structure from TikTok API response
 * Pattern: Similar to FacebookOrderDto but with TikTok-specific nested structure
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TikTokOrderDto {

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

    @JsonProperty("tiktok_data")
    private TikTokData tiktokData;

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

    /**
     * TikTok Data - Nested structure containing order_detail and return_refund
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TikTokData {

        @JsonProperty("order_detail")
        private TikTokOrderDetail orderDetail;

        @JsonProperty("return_refund")
        private TikTokReturnRefund returnRefund;
    }

    // ========== HELPER METHODS ==========

    public TikTokOrderDetail getOrderDetail() {
        return tiktokData != null ? tiktokData.getOrderDetail() : null;
    }

    public TikTokReturnRefund getReturnRefund() {
        return tiktokData != null ? tiktokData.getReturnRefund() : null;
    }

    public boolean hasTikTokData() {
        return tiktokData != null;
    }

    public boolean hasOrderDetail() {
        return getOrderDetail() != null;
    }

    public boolean hasReturnRefund() {
        return getReturnRefund() != null;
    }

    public String getOrderIdSafe() {
        if (hasOrderDetail()) {
            return getOrderDetail().getId();
        }
        return orderId;
    }

    public String getTrackingNumberSafe() {
        if (hasOrderDetail()) {
            return getOrderDetail().getTrackingNumber();
        }
        return trackingNumber;
    }

    public Integer getStatusSafe() {
        return status;
    }

    public boolean isLivestreamOrder() {
        return isLivestream != null && isLivestream == 1;
    }
}