package com.guno.dataimport.dto.platform.shopee;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * ShopeeOrderDetail - Main order details from Shopee
 * Maps to "shopee_data.order_detail" in order JSON
 *
 * Location: src/main/java/com/guno/dataimport/dto/platform/shopee/ShopeeOrderDetail.java
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ShopeeOrderDetail {

    @JsonProperty("order_sn")
    private String orderSn;

    @JsonProperty("order_status")
    private String orderStatus;

    @JsonProperty("region")
    private String region;

    @JsonProperty("currency")
    private String currency;

    @JsonProperty("cod")
    private Boolean cod;

    @JsonProperty("total_amount")
    private Long totalAmount;

    @JsonProperty("actual_shipping_fee")
    private Long actualShippingFee;

    @JsonProperty("estimated_shipping_fee")
    private Long estimatedShippingFee;

    @JsonProperty("reverse_shipping_fee")
    private Long reverseShippingFee;

    @JsonProperty("payment_method")
    private String paymentMethod;

    @JsonProperty("shipping_carrier")
    private String shippingCarrier;

    @JsonProperty("create_time")
    private Long createTime;

    @JsonProperty("update_time")
    private Long updateTime;

    @JsonProperty("pay_time")
    private Long payTime;

    @JsonProperty("ship_by_date")
    private Long shipByDate;

    @JsonProperty("pickup_done_time")
    private Long pickupDoneTime;

    @JsonProperty("days_to_ship")
    private Integer daysToShip;

    @JsonProperty("cancel_reason")
    private String cancelReason;

    @JsonProperty("buyer_cancel_reason")
    private String buyerCancelReason;

    @JsonProperty("cancel_by")
    private String cancelBy;

    @JsonProperty("message_to_seller")
    private String messageToSeller;

    @JsonProperty("note")
    private String note;

    @JsonProperty("recipient_address")
    private ShopeeRecipientAddress recipientAddress;

    @JsonProperty("item_list")
    @Builder.Default
    private List<ShopeeItem> itemList = new ArrayList<>();

    @JsonProperty("package_list")
    @Builder.Default
    private List<ShopeePackage> packageList = new ArrayList<>();

    @JsonProperty("split_up")
    private Boolean splitUp;

    @JsonProperty("booking_sn")
    private String bookingSn;

    @JsonProperty("fulfillment_flag")
    private String fulfillmentFlag;

    @JsonProperty("checkout_shipping_carrier")
    private String checkoutShippingCarrier;

    @JsonProperty("order_chargeable_weight_gram")
    private Integer orderChargeableWeightGram;

    @JsonProperty("actual_shipping_fee_confirmed")
    private Boolean actualShippingFeeConfirmed;

    @JsonProperty("is_buyer_shop_collection")
    private Boolean isBuyerShopCollection;

    @JsonProperty("goods_to_declare")
    private Boolean goodsToDeclare;

    @JsonProperty("note_update_time")
    private Long noteUpdateTime;

    @JsonProperty("hot_listing_order")
    private Boolean hotListingOrder;

    @JsonProperty("dropshipper_phone")
    private String dropshipperPhone;

    @JsonProperty("advance_package")
    private Boolean advancePackage;

    @JsonProperty("invoice_data")
    private Object invoiceData;

    // Helper methods
    public boolean isCodOrder() {
        return Boolean.TRUE.equals(cod);
    }

    public Double getTotalAmountAsDouble() {
        return totalAmount != null ? totalAmount.doubleValue() : 0.0;
    }

    public Double getActualShippingFeeAsDouble() {
        return actualShippingFee != null ? actualShippingFee.doubleValue() : 0.0;
    }

    public boolean hasItems() {
        return itemList != null && !itemList.isEmpty();
    }

    public int getItemCount() {
        return itemList != null ? itemList.size() : 0;
    }

    public boolean hasPackages() {
        return packageList != null && !packageList.isEmpty();
    }

    public String getFirstPackageNumber() {
        if (hasPackages()) {
            return packageList.get(0).getPackageNumber();
        }
        return null;
    }

    public String getLogisticsStatus() {
        if (hasPackages()) {
            return packageList.get(0).getLogisticsStatus();
        }
        return null;
    }

    public String getCustomerName() {
        return recipientAddress != null ? recipientAddress.getName() : null;
    }

    public String getCustomerPhone() {
        return recipientAddress != null ? recipientAddress.getPhone() : null;
    }

    public String getProvince() {
        return recipientAddress != null ? recipientAddress.getState() : null;
    }

    public String getDistrict() {
        return recipientAddress != null ? recipientAddress.getCity() : null;
    }

    public String getWard() {
        return recipientAddress != null ? recipientAddress.getDistrict() : null;
    }
}