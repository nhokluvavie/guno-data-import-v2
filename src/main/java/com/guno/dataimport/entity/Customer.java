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
    @Builder.Default private String phoneHash = "";
    @Builder.Default private String emailHash = "";
    @Builder.Default private String gender = "";
    @Builder.Default private String ageGroup = "";
    @Builder.Default private String customerSegment = "";
    @Builder.Default private String customerTier = "";
    @Builder.Default private String acquisitionChannel = "";
    @Builder.Default private LocalDateTime firstOrderDate = LocalDateTime.now();
    @Builder.Default private LocalDateTime lastOrderDate =  LocalDateTime.now();

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

    @Builder.Default private String favoriteCategory = "";
    @Builder.Default private String favoriteBrand = "";
    @Builder.Default private String preferredPaymentMethod = "";
    @Builder.Default private String preferredPlatform = "";
    @Builder.Default private String primaryShippingProvince = "";

    @Builder.Default private Boolean shipsToMultipleProvinces = false;
    @Builder.Default private Integer loyaltyPoints = 0;
    @Builder.Default private Integer referralCount = 0;
    @Builder.Default private Boolean isReferrer = false;

    @Builder.Default private String customerName = "";
}