package com.guno.dataimport.dto.platform.facebook;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.ArrayList;
import java.util.List;

/**
 * Facebook Order DTO - Main order entity from Facebook API
 * Note: Order data is nested in "data" field in API response
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
    private Integer status; // Main order status

    @JsonProperty("data")
    private FacebookOrderData data; // Nested order data

    // Helper methods to access nested data
    public String getOrderId() {
        return data != null ? data.getId().toString() : orderId;
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

    public String getCreatedAt() {
        return data != null ? data.getUpdateAt() : null;
    }

    public String getBillPhoneNumber() {
        return data != null ? data.getBillPhoneNumber() : null;
    }

    public String getNewProvinceName() {
        return data != null ? data.getShippingAddress() != null ?
                data.getShippingAddress().getProvinceName() : null : null;
    }

    public String getNewDistrictName() {
        return data != null ? data.getShippingAddress() != null ?
                data.getShippingAddress().getDistrictName() : null : null;
    }

    public String getAdId() {
        return data != null ? data.getAdId() : null;
    }

    public Integer getStatus() {
        return status; // Main status
    }

    public Integer getNestedStatus() {
        return data != null ? data.getStatus() : null; // Nested status
    }

    public String getStatusName() {
        return data != null ? data.getStatusName() : null;
    }

    public boolean isCodOrder() {
        return getCod() != null && getCod() > 0;
    }

    public double getTotalAmountAsDouble() {
        Long total = getTotalPriceAfterSubDiscount();
        return total != null ? total.doubleValue() : 0.0;
    }

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

        @JsonProperty("update_at")
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

        @Data
        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class ShippingAddress {
            @JsonProperty("province_name")
            private String provinceName;

            @JsonProperty("district_name")
            private String districtName;
        }
    }
}