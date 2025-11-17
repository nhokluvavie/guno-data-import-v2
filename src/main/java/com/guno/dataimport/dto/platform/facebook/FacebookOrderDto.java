package com.guno.dataimport.dto.platform.facebook;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/**
 * FacebookOrderDto - FIXED with geography helpers
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FacebookOrderDto {

    @JsonProperty("order_id")
    private String orderId;

    @JsonProperty("status")
    private Integer status;

    @JsonProperty("data")
    private FacebookOrderData data;

    @JsonProperty("inserted_at")
    private String insertedAt;

    @JsonProperty("tiktok_data")
    private TikTokData tiktokData;

    @JsonProperty("source")
    private String source;

    // ========== BASIC HELPERS ==========

    public String getOrderId() {
        return data != null ? data.getId() : orderId;
    }

    public Long getCod() {
        return data != null ? data.getCod() : null;
    }

    public Long getTax() {
        return data != null ? data.getTax() : null;
    }

    public Long getCash() {
        return data != null ? data.getCash() : null;
    }

    public Long getTotalPriceAfterSubDiscount() {
        return data != null ? data.getTotalPriceAfterSubDiscount() : null;
    }

    public Long getDiscount() {
        return data != null ? data.getTotalDiscount() : null;
    }

    public Long getShippingFee() {
        return data != null ? data.getShippingFee() : null;
    }

    public List<FacebookItemDto> getItems() {
        return data != null ? data.getItems() : new ArrayList<>();
    }

    public List<Tag> getTags() {
        return data != null ? data.getTags() : new ArrayList<>();
    }

    public FacebookCustomer getCustomer() {
        return data != null ? data.getCustomer() : null;
    }

    public LocalDateTime getCreatedAt() {
        return data != null ? LocalDateTime.parse(data.getUpdateAt()) : null;
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

    public String getBillPhoneNumber() {
        return data != null ? data.getBillPhoneNumber() : null;
    }

    public String getPageId() {
        return data != null && data.getPage() != null ? data.getPage().getName() : null;
    }

    public String getAccountName() {
        return data != null ? data.getAccountName() : null;
    }

    public AssigningSeller getAssigningSeller() {
        return data != null ? data.getAssigningSeller() : null;
    }

    public Integer getSubStatus() {
        return data != null ? data.getSubStatus() : null;
    }

    public List<TrackingHistory> getTrackingHistories() {
        return data != null ? data.getTrackingHistories() : new ArrayList<>();
    }

    public boolean isCodOrder() {
        Long cod = getCod();
        return cod != null && cod > 0;
    }

    public Double getTotalAsDouble() {
        Long total = getTotalPriceAfterSubDiscount();
        return total != null ? total.doubleValue() : 0.0;
    }

    public List<ChangedLog> getHistories() {
        return data != null ? data.getHistories() : new ArrayList<>();
    }

    public Partner getPartner() {
        return data != null ? data.getPartner() : null;
    }

    public AdvancedPlatformFee getAdvancedPlatformFee() {
        return data != null ? data.getAdvancedPlatformFee() : null;
    }

    // ========== GEOGRAPHY HELPERS ==========

    public String getNewProvinceName() {
        return data != null && data.getShippingAddress() != null
                ? data.getShippingAddress().getProvinceName() : null;
    }

    public String getNewDistrictName() {
        return data != null && data.getShippingAddress() != null
                ? data.getShippingAddress().getDistrictName() : null;
    }

    public ShippingAddress getShippingAddress() {
        return data != null ? data.getShippingAddress() : null;
    }

    public String getAdId() {
        return data != null ? data.getAdId() : null;
    }

    /** Safe province - returns "Unknown" if null/empty */
    public String getProvinceSafe() {
        String p = getNewProvinceName();
        return (p != null && !p.trim().isEmpty()) ? p.trim() : "Unknown";
    }

    /** Safe district - returns "Unknown" if null/empty */
    public String getDistrictSafe() {
        String d = getNewDistrictName();
        return (d != null && !d.trim().isEmpty()) ? d.trim() : "Unknown";
    }

    public boolean hasAd() {
        String adId = getAdId();
        return adId != null && !adId.trim().isEmpty() && !"null".equalsIgnoreCase(adId);
    }

    // ========== TIKTOK HELPERS ==========

    public boolean isTikTokOrder() {
        return "tiktok".equalsIgnoreCase(source) || "Tiktok".equalsIgnoreCase(source);
    }

    public boolean hasRefundData() {
        return tiktokData != null && tiktokData.getReturnRefund() != null;
    }

    public boolean isRefunded() {
        if (!hasRefundData()) return false;
        String returnType = tiktokData.getReturnRefund().getReturnType();
        return returnType != null && (returnType.contains("REFUND") || returnType.equals("RETURN_AND_REFUND"));
    }

    public Double getRefundAmount() {
        if (!hasRefundData()) return null;
        ReturnRefund refund = tiktokData.getReturnRefund();
        if (refund.getRefundAmount() == null) return null;
        String refundTotal = refund.getRefundAmount().getRefundTotal();
        try {
            return refundTotal != null ? Double.parseDouble(refundTotal) : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public String getRefundDate() {
        if (!hasRefundData()) return "";
        Long updateTime = tiktokData.getReturnRefund().getUpdateTime();
        if (updateTime == null) return "";
        try {
            return LocalDateTime.ofInstant(Instant.ofEpochSecond(updateTime), ZoneId.of("Asia/Ho_Chi_Minh")).toString();
        } catch (Exception e) {
            return "";
        }
    }

    public boolean isExchangeOrder() {
        List<Tag> tags = getTags();
        if (tags == null || tags.isEmpty()) return false;
        return tags.stream().anyMatch(tag -> tag.getName() != null && tag.getName().contains("GH1P"));
    }

    // ========== NESTED CLASSES ==========

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class FacebookOrderData {
        @JsonProperty("id")
        private String id;
        @JsonProperty("cod")
        private Long cod;
        @JsonProperty("tax")
        private Long tax;
        @JsonProperty("cash")
        private Long cash;
        @JsonProperty("total_price_after_sub_discount")
        private Long totalPriceAfterSubDiscount;
        @JsonProperty("total_discount")
        private Long totalDiscount;
        @JsonProperty("shipping_fee")
        private Long shippingFee;
        @JsonProperty("items")
        @Builder.Default
        private List<FacebookItemDto> items = new ArrayList<>();
        @JsonProperty("tags")
        @Builder.Default
        private List<Tag> tags = new ArrayList<>();
        @JsonProperty("customer")
        private FacebookCustomer customer;
        @JsonProperty("updated_at")
        private String updateAt;
        @JsonProperty("bill_phone_number")
        private String billPhoneNumber;
        @JsonProperty("shipping_address")
        private ShippingAddress shippingAddress;
        @JsonProperty("ad_id")
        private String adId;
        @JsonProperty("status")
        private Integer status;
        @JsonProperty("status_name")
        private String statusName;
        @JsonProperty("account_name")
        private String accountName;
        @JsonProperty("assigning_seller")
        private AssigningSeller assigningSeller;
        @JsonProperty("sub_status")
        private Integer subStatus;
        @JsonProperty("tracking_histories")
        @Builder.Default
        private List<TrackingHistory> trackingHistories = new ArrayList<>();
        @JsonProperty("page")
        private Page page;
        @JsonProperty("histories")
        @Builder.Default
        private List<ChangedLog> histories = new ArrayList<>();
        @JsonProperty("partner")
        private Partner partner;
        @JsonProperty("note")
        private String note;
        @JsonProperty("advanced_platform_fee")
        private AdvancedPlatformFee advancedPlatformFee;
        @JsonProperty("returned_reason")
        private String returnedReason;
        @JsonProperty("is_livestream")
        private boolean isLivestream;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Tag {
        @JsonProperty("id")
        private Integer id;
        @JsonProperty("name")
        private String name;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TikTokData {
        @JsonProperty("return_refund")
        private ReturnRefund returnRefund;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ReturnRefund {
        @JsonProperty("role")
        private String role;
        @JsonProperty("order_id")
        private String orderId;
        @JsonProperty("return_id")
        private String returnId;
        @JsonProperty("create_time")
        private Long createTime;
        @JsonProperty("return_type")
        private String returnType;
        @JsonProperty("update_time")
        private Long updateTime;
        @JsonProperty("refund_amount")
        private RefundAmount refundAmount;
        @JsonProperty("return_method")
        private String returnMethod;
        @JsonProperty("return_reason")
        private String returnReason;
        @JsonProperty("return_status")
        private String returnStatus;
        @JsonProperty("return_reason_text")
        private String returnReasonText;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RefundAmount {
        @JsonProperty("currency")
        private String currency;
        @JsonProperty("refund_tax")
        private String refundTax;
        @JsonProperty("refund_total")
        private String refundTotal;
        @JsonProperty("refund_subtotal")
        private String refundSubtotal;
        @JsonProperty("refund_shipping_fee")
        private String refundShippingFee;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ShippingAddress {
        @JsonProperty("province_name")
        private String provinceName;
        @JsonProperty("district_name")
        private String districtName;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AssigningSeller {
        @JsonProperty("id")
        private String id;
        @JsonProperty("name")
        private String name;
        @JsonProperty("email")
        private String email;
        @JsonProperty("fb_id")
        private String fbId;
        @JsonProperty("phone_number")
        private String phoneNumber;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TrackingHistory {
        @JsonProperty("partner_status")
        private String partnerStatus;
        @JsonProperty("status")
        private String status;
        @JsonProperty("update_at")
        private String updateAt;
        @JsonProperty("tracking_id")
        private String trackingId;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Page {
        @JsonProperty("id")
        private String id;
        @JsonProperty("name")
        private String name;
        @JsonProperty("username")
        private String username;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Partner {
        @JsonProperty("is_returned")
        private Boolean isReturned;
        @JsonProperty("partner_status")
        private String partnerStatus;
        @JsonProperty("extend_update")
        @Builder.Default
        private List<ExtendUpdate> extendUpdate = new ArrayList<>();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ExtendUpdate {
        @JsonProperty("status")
        private String status;
        @JsonProperty("updated_at")
        private String updatedAt;
        @JsonProperty("tracking_id")
        private String trackingId;
    }
}