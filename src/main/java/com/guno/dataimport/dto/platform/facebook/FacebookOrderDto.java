package com.guno.dataimport.dto.platform.facebook;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Facebook Order DTO - Main order entity from Facebook API
 * UPDATED: Added seller fields (account_name, assigning_seller)
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

    // ========== HELPER METHODS ==========

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

    public FacebookCustomer getCustomer() {
        return data != null ? data.getCustomer() : null;
    }

    public LocalDateTime getCreatedAt() {
        return data != null ? LocalDateTime.parse(data.getUpdateAt()) : null;
    }

    public String getBillPhoneNumber() {
        return data != null ? data.getBillPhoneNumber() : null;
    }

    public String getNewProvinceName() {
        return data != null && data.getShippingAddress() != null
                ? data.getShippingAddress().getProvinceName() : null;
    }

    public String getNewDistrictName() {
        return data != null && data.getShippingAddress() != null
                ? data.getShippingAddress().getDistrictName() : null;
    }

    public String getAdId() {
        return data != null ? data.getAdId() : null;
    }

    public Integer getNestedStatus() {
        return data != null ? data.getStatus() : null;
    }

    public String getStatusName() {
        return data != null ? data.getStatusName() : null;
    }

    // NEW - Seller fields
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

    public String getPageId() {
        return data != null && data.getPage() != null ? data.getPage().getId() : null;
    }

    public boolean isCodOrder() {
        return getCod() != null && getCod() > 0;
    }

    public double getTotalAmountAsDouble() {
        Long total = getTotalPriceAfterSubDiscount();
        return total != null ? total.doubleValue() : 0.0;
    }

    public List<ChangedLog> getHistories() {
        return data != null ? data.getHistories() : new ArrayList<>();
    }

    public Partner getPartner() {
        return data != null ? data.getPartner() : null;
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

        // NEW FIELDS
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