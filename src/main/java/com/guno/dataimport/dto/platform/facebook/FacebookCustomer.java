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
 * Facebook Customer DTO - Customer information from Facebook orders
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FacebookCustomer {

    @JsonProperty("id")
    private String id;

    @JsonProperty("name")
    private String name;

    @JsonProperty("gender")
    private String gender;

    @JsonProperty("username")
    private String username;

    @JsonProperty("customer_id")
    private String customerId;

    // Order statistics
    @JsonProperty("order_count")
    private Integer orderCount;

    @JsonProperty("succeed_order_count")
    private Integer succeedOrderCount;

    @JsonProperty("returned_order_count")
    private Integer returnedOrderCount;

    @JsonProperty("purchased_amount")
    private Long purchasedAmount;

    @JsonProperty("total_amount_referred")
    private Long totalAmountReferred;

    // Loyalty and referrals
    @JsonProperty("reward_point")
    private Integer rewardPoint;

    @JsonProperty("used_reward_point")
    private Integer usedRewardPoint;

    @JsonProperty("referral_code")
    private String referralCode;

    @JsonProperty("count_referrals")
    private Integer countReferrals;

    @JsonProperty("is_referrer")
    private Boolean isReferrer;

    // Contact information
    @JsonProperty("phone_numbers")
    @Builder.Default
    private List<String> phoneNumbers = new ArrayList<>();

    @JsonProperty("emails")
    @Builder.Default
    private List<String> emails = new ArrayList<>();

    // Dates
    @JsonProperty("date_of_birth")
    private String dateOfBirth;

    @JsonProperty("inserted_at")
    private String insertedAt;

    @JsonProperty("last_order_at")
    private String lastOrderAt;

    // Order sources and preferences
    @JsonProperty("order_sources")
    @Builder.Default
    private List<String> orderSources = new ArrayList<>();

    @JsonProperty("conversation_tags")
    private String conversationTags;

    // Addresses
    @JsonProperty("shop_customer_addresses")
    @Builder.Default
    private List<FacebookShippingAddress> shopCustomerAddresses = new ArrayList<>();

    // Financial flags
    @JsonProperty("current_debts")
    private Long currentDebts;

    @JsonProperty("is_adjust_debts")
    private Boolean isAdjustDebts;

    @JsonProperty("active_levera_pay")
    private Boolean activeLeveraPay;

    @JsonProperty("is_discount_by_level")
    private Boolean isDiscountByLevel;

    // Helper methods
    public String getPrimaryPhone() {
        return phoneNumbers != null && !phoneNumbers.isEmpty() ? phoneNumbers.get(0) : null;
    }

    public String getPrimaryEmail() {
        return emails != null && !emails.isEmpty() ? emails.get(0) : null;
    }

    public double getPurchasedAmountAsDouble() {
        return purchasedAmount != null ? purchasedAmount.doubleValue() : 0.0;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class FacebookShippingAddress {

        @JsonProperty("id")
        private String id;

        @JsonProperty("name")
        private String name;

        @JsonProperty("phone")
        private String phone;

        @JsonProperty("address")
        private String address;

        @JsonProperty("city_id")
        private Integer cityId;

        @JsonProperty("city_name")
        private String cityName;

        @JsonProperty("district_id")
        private Integer districtId;

        @JsonProperty("district_name")
        private String districtName;

        @JsonProperty("ward_id")
        private Integer wardId;

        @JsonProperty("ward_name")
        private String wardName;

        @JsonProperty("is_default")
        private Boolean isDefault;
    }
}