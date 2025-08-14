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

    private String productId;
    private String variationId;
    private String barcode;
    private String productName;
    private String productDescription;
    private String brand;
    private String model;

    // Category hierarchy
    private String categoryLevel1;
    private String categoryLevel2;
    private String categoryLevel3;
    private String categoryPath;

    // Product attributes
    private String color;
    private String size;
    private String material;
    private String dimensions;

    @Builder.Default private Integer weightGram = 0;
    @Builder.Default private Double costPrice = 0.0;
    @Builder.Default private Double retailPrice = 0.0;
    @Builder.Default private Double originalPrice = 0.0;

    private String priceRange;

    @Builder.Default private Boolean isActive = true;
    @Builder.Default private Boolean isFeatured = false;
    @Builder.Default private Boolean isSeasonal = false;
    @Builder.Default private Boolean isNewArrival = false;
    @Builder.Default private Boolean isBestSeller = false;

    private String primaryImageUrl;
    @Builder.Default private Integer imageCount = 0;

    // SEO fields
    private String seoTitle;
    private String seoKeywords;
}