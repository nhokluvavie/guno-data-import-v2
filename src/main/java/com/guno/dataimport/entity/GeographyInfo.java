package com.guno.dataimport.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * GeographyInfo Entity - Maps to tbl_geography_info table
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GeographyInfo {

    private String orderId;
    private Long geographyKey;

    // Location hierarchy
    private String countryCode;
    private String countryName;
    private String regionCode;
    private String regionName;
    private String provinceCode;
    private String provinceName;
    private String provinceType;
    private String districtCode;
    private String districtName;
    private String districtType;
    private String wardCode;
    private String wardName;
    private String wardType;

    // Geographic characteristics
    @Builder.Default private Boolean isUrban = false;
    @Builder.Default private Boolean isMetropolitan = false;
    @Builder.Default private Boolean isCoastal = false;
    @Builder.Default private Boolean isBorder = false;

    private String economicTier;
    private String populationDensity;
    private String incomeLevel;

    // Shipping related
    private String shippingZone;
    private String deliveryComplexity;
    @Builder.Default private Integer standardDeliveryDays = 0;
    @Builder.Default private Boolean expressDeliveryAvailable = false;

    // Coordinates
    @Builder.Default private Double latitude = 0.0;
    @Builder.Default private Double longitude = 0.0;
}