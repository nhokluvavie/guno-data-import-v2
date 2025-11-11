package com.guno.dataimport.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.LocalDateTime;

/**
 * Customer Entity - Maps to tbl_customer table
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Customer {

    private String customerId;
    private Long customerKey;
    private String platformCustomerId;
    private String phoneHash;
    private String emailHash;
    private String gender;
    private String ageGroup;
    private String customerSegment;
    private String customerTier;
    private String acquisitionChannel;
    private LocalDateTime firstOrderDate;
    private LocalDateTime lastOrderDate;

    @Builder.Default private Integer totalOrders = 0;
    @Builder.Default private Double totalSpent = 0.0;
    @Builder.Default private Double averageOrderValue = 0.0;
    @Builder.Default private Integer totalItemsPurchased = 0;
    @Builder.Default private Integer daysSinceFirstOrder = 0;
    @Builder.Default private Integer daysSinceLastOrder = 0;
    @Builder.Default private Double purchaseFrequencyDays = 0.0;
    @Builder.Default private Double returnRate = 0.0;
    @Builder.Default private Double cancellationRate = 0.0;
    @Builder.Default private Double codPreferenceRate = 0.0;

    private String favoriteCategory;
    private String favoriteBrand;
    private String preferredPaymentMethod;
    private String preferredPlatform;
    private String primaryShippingProvince;

    @Builder.Default private Boolean shipsToMultipleProvinces = false;
    @Builder.Default private Integer loyaltyPoints = 0;
    @Builder.Default private Integer referralCount = 0;
    @Builder.Default private Boolean isReferrer = false;

    private String customerName;
}