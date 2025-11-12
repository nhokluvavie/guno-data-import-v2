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
    @Builder.Default private String countryCode = "";
    @Builder.Default private String countryName = "";
    @Builder.Default private String regionCode = "";
    @Builder.Default private String regionName = "";
    @Builder.Default private String provinceCode = "";
    @Builder.Default private String provinceName = "";
    @Builder.Default private String provinceType = "";
    @Builder.Default private String districtCode = "";
    @Builder.Default private String districtName = "";
    @Builder.Default private String districtType = "";
    @Builder.Default private String wardCode = "";
    @Builder.Default private String wardName = "";
    @Builder.Default private String wardType = "";

    // Geographic characteristics
    @Builder.Default private Boolean isUrban = false;
    @Builder.Default private Boolean isMetropolitan = false;
    @Builder.Default private Boolean isCoastal = false;
    @Builder.Default private Boolean isBorder = false;

    @Builder.Default private String economicTier = "";
    @Builder.Default private String populationDensity = "";
    @Builder.Default private String incomeLevel = "";

    // Shipping related
    @Builder.Default private String shippingZone = "";
    @Builder.Default private String deliveryComplexity = "";
    @Builder.Default private Integer standardDeliveryDays = 0;
    @Builder.Default private Boolean expressDeliveryAvailable = false;

    // Coordinates
    @Builder.Default private Double latitude = 0.0;
    @Builder.Default private Double longitude = 0.0;
}