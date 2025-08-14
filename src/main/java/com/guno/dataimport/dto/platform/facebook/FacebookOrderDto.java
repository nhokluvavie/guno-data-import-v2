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
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FacebookOrderDto {

    @JsonProperty("id")
    private String id;

    @JsonProperty("cod")
    private Long cod;

    @JsonProperty("tax")
    private Long tax;

    @JsonProperty("cash")
    private Long cash;

    @JsonProperty("link")
    private String link;

    @JsonProperty("note")
    private String note;

    @JsonProperty("type")
    private String type;

    @JsonProperty("status")
    private Integer status;

    // Financial data
    @JsonProperty("total_price_after_sub_discount")
    private Long totalPriceAfterSubDiscount;

    @JsonProperty("discount")
    private Long discount;

    @JsonProperty("shipping_fee")
    private Long shippingFee;

    @JsonProperty("surcharge")
    private Long surcharge;

    @JsonProperty("buyer_total_amount")
    private Long buyerTotalAmount;

    // Items and customer
    @JsonProperty("items")
    @Builder.Default
    private List<FacebookItemDto> items = new ArrayList<>();

    @JsonProperty("customer")
    private FacebookCustomer customer;

    // Address and contact info
    @JsonProperty("bill_phone_number")
    private String billPhoneNumber;

    @JsonProperty("bill_email")
    private String billEmail;

    @JsonProperty("new_province_name")
    private String newProvinceName;

    @JsonProperty("new_district_name")
    private String newDistrictName;

    @JsonProperty("new_commune_id")
    private String newCommuneId;

    @JsonProperty("new_province_id")
    private String newProvinceId;

    @JsonProperty("new_full_address")
    private String newFullAddress;

    // Order source and tracking
    @JsonProperty("order_sources_name")
    private String orderSourcesName;

    @JsonProperty("ad_id")
    private String adId;

    @JsonProperty("system_id")
    private Long systemId;

    @JsonProperty("note_print")
    private String notePrint;

    // Timestamps
    @JsonProperty("created_at")
    private String createdAt;

    @JsonProperty("updated_at")
    private String updatedAt;

    @JsonProperty("estimate_delivery_date")
    private String estimateDeliveryDate;

    // Helper methods
    public double getTotalAmountAsDouble() {
        return totalPriceAfterSubDiscount != null ? totalPriceAfterSubDiscount.doubleValue() : 0.0;
    }

    public boolean isCodOrder() {
        return cod != null && cod > 0;
    }

    public String getOrderId() {
        return id != null ? id.toString() : null;
    }
}