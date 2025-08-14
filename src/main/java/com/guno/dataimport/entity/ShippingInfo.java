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
    private String providerId;
    private String providerName;
    private String providerType;
    private String providerTier;

    // Service details
    private String serviceType;
    private String serviceTier;
    private String deliveryCommitment;
    private String shippingMethod;
    private String pickupType;
    private String deliveryType;

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
    private String coverageProvinces;
    @Builder.Default private Boolean coverageNationwide = false;
    @Builder.Default private Boolean coverageInternational = false;
}