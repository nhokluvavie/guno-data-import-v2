package com.guno.dataimport.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Product Entity - Maps to tbl_product table
 * Uses composite primary key (sku, platformProductId)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Product {

    // Composite Primary Key
    private String sku;
    private String platformProductId;

    @Builder.Default private String productId = "";
    @Builder.Default private String variationId = "";
    @Builder.Default private String barcode = "";
    @Builder.Default private String productName = "";
    @Builder.Default private String productDescription = "";
    @Builder.Default private String brand = "";
    @Builder.Default private String model = "";

    // Category hierarchy
    @Builder.Default private String categoryLevel1 = "";
    @Builder.Default private String categoryLevel2 = "";
    @Builder.Default private String categoryLevel3 = "";
    @Builder.Default private String categoryPath = "";

    // Product attributes
    @Builder.Default private String color = "";
    @Builder.Default private String size = "";
    @Builder.Default private String material = "";
    @Builder.Default private String dimensions = "";

    @Builder.Default private Integer weightGram = 0;
    @Builder.Default private Double costPrice = 0.0;
    @Builder.Default private Double retailPrice = 0.0;
    @Builder.Default private Double originalPrice = 0.0;

    @Builder.Default private String priceRange = "";

    @Builder.Default private Boolean isActive = true;
    @Builder.Default private Boolean isFeatured = false;
    @Builder.Default private Boolean isSeasonal = false;
    @Builder.Default private Boolean isNewArrival = false;
    @Builder.Default private Boolean isBestSeller = false;

    @Builder.Default private String primaryImageUrl = "";
    @Builder.Default private Integer imageCount = 0;

    // SEO fields
    @Builder.Default private String seoTitle = "";
    @Builder.Default private String seoKeywords = "";

    @Builder.Default private String skuGroup = "";
}