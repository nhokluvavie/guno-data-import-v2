package com.guno.dataimport.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ShippingInfo Entity - Maps to tbl_shipping_info table
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShippingInfo {

    private String orderId;
    private Long shippingKey;

    // Provider information
    @Builder.Default private String providerId = "";
    @Builder.Default private String providerName = "";
    @Builder.Default private String providerType = "";
    @Builder.Default private String providerTier = "";

    // Service details
    @Builder.Default private String serviceType = "";
    @Builder.Default private String serviceTier = "";
    @Builder.Default private String deliveryCommitment = "";
    @Builder.Default private String shippingMethod = "";
    @Builder.Default private String pickupType = "";
    @Builder.Default private String deliveryType = "";

    // Fee structure
    @Builder.Default private Double baseFee = 0.0;
    @Builder.Default private Double weightBasedFee = 0.0;
    @Builder.Default private Double distanceBasedFee = 0.0;
    @Builder.Default private Double codFee = 0.0;
    @Builder.Default private Double insuranceFee = 0.0;

    // Service capabilities
    @Builder.Default private Boolean supportsCod = false;
    @Builder.Default private Boolean supportsInsurance = false;
    @Builder.Default private Boolean supportsFragile = false;
    @Builder.Default private Boolean supportsRefrigerated = false;
    @Builder.Default private Boolean providesTracking = false;
    @Builder.Default private Boolean providesSmsUpdates = false;

    // Performance metrics
    @Builder.Default private Double averageDeliveryDays = 0.0;
    @Builder.Default private Double onTimeDeliveryRate = 0.0;
    @Builder.Default private Double successDeliveryRate = 0.0;
    @Builder.Default private Double damageRate = 0.0;

    // Coverage
    @Builder.Default private String coverageProvinces = "";
    @Builder.Default private Boolean coverageNationwide = false;
    @Builder.Default private Boolean coverageInternational = false;
}