package com.guno.dataimport.dto.platform.facebook;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Facebook Item DTO - Order item details from Facebook API
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FacebookItemDto {

    @JsonProperty("id")
    private Long id;

    @JsonProperty("name")
    private String name;

    @JsonProperty("product_name")
    private String productName;

    @JsonProperty("price")
    private Long price;

    @JsonProperty("quantity")
    private Integer quantity;

    @JsonProperty("total_price")
    private Long totalPrice;

    // Product identification
    @JsonProperty("product_id")
    private String productId;

    @JsonProperty("variant_id")
    private String variantId;

    @JsonProperty("sku")
    private String sku;

    @JsonProperty("barcode")
    private String barcode;

    // Product details
    @JsonProperty("category")
    private String category;

    @JsonProperty("brand")
    private String brand;

    @JsonProperty("model")
    private String model;

    @JsonProperty("color")
    private String color;

    @JsonProperty("size")
    private String size;

    @JsonProperty("material")
    private String material;

    @JsonProperty("weight_gram")
    private Integer weightGram;

    @JsonProperty("dimensions")
    private String dimensions;

    // Pricing
    @JsonProperty("cost_price")
    private Long costPrice;

    @JsonProperty("retail_price")
    private Long retailPrice;

    @JsonProperty("original_price")
    private Long originalPrice;

    @JsonProperty("discount_amount")
    private Long discountAmount;

    @JsonProperty("discount_rate")
    private Double discountRate;

    // Images and media
    @JsonProperty("image_url")
    private String imageUrl;

    @JsonProperty("primary_image_url")
    private String primaryImageUrl;

    @JsonProperty("image_count")
    private Integer imageCount;

    // Status and flags
    @JsonProperty("status")
    private String status;

    @JsonProperty("is_active")
    private Boolean isActive;

    @JsonProperty("is_featured")
    private Boolean isFeatured;

    @JsonProperty("is_new_arrival")
    private Boolean isNewArrival;

    @JsonProperty("is_best_seller")
    private Boolean isBestSeller;

    @JsonProperty("note")
    private String note;

    @JsonProperty("sequence")
    private Integer sequence;

    // Helper methods
    public double getPriceAsDouble() {
        return price != null ? price.doubleValue() : 0.0;
    }

    public double getTotalPriceAsDouble() {
        return totalPrice != null ? totalPrice.doubleValue() : 0.0;
    }

    public String getProductKey() {
        return sku != null ? sku : (productId != null ? productId : id.toString());
    }

    public int getQuantityOrDefault() {
        return quantity != null ? quantity : 1;
    }
}