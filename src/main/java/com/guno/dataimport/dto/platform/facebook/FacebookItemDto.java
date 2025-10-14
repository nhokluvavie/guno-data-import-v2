package com.guno.dataimport.dto.platform.facebook;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Facebook Item DTO - Order item details from Facebook API
 * Based on actual JSON structure from facebook_order.json
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FacebookItemDto {

    @JsonProperty("id")
    private Long id;

    @JsonProperty("note")
    private String note;

    @JsonProperty("quantity")
    private Integer quantity;

    @JsonProperty("product_id")
    private String productId;

    @JsonProperty("variation_id")
    private String variationId;

    @JsonProperty("note_product")
    private String noteProduct;

    @JsonProperty("total_discount")
    private Long totalDiscount;

    @JsonProperty("variation_info")
    private VariationInfo variationInfo;

    @JsonProperty("return_quantity")
    private Integer returnQuantity;

    @JsonProperty("returning_quantity")
    private Integer returningQuantity;


    // Helper methods
    public String getSku() {
        return variationInfo != null ? variationInfo.getDisplayId() : null;
    }

    public String getProductKey() {
        return getSku() != null ? getSku() : (productId != null ? productId : id.toString());
    }

    public String getName() {
        return variationInfo != null ? variationInfo.getName() : null;
    }

    public Double getPriceAsDouble() {
        return variationInfo != null && variationInfo.getRetailPrice() != null ?
                variationInfo.getRetailPrice().doubleValue() : 0.0;
    }

    public int getQuantityOrDefault() {
        return quantity != null ? quantity : 1;
    }

    public String getColor() {
        if (variationInfo != null && variationInfo.getFields() != null) {
            return variationInfo.getFields().stream()
                    .filter(field -> "Màu".equals(field.getName()))
                    .map(VariationField::getValue)
                    .findFirst()
                    .orElse(null);
        }
        return null;
    }

    public String getSize() {
        if (variationInfo != null && variationInfo.getFields() != null) {
            return variationInfo.getFields().stream()
                    .filter(field -> "Sz".equals(field.getName()))
                    .map(VariationField::getValue)
                    .findFirst()
                    .orElse(null);
        }
        return null;
    }

    public String getBarcode() {
        return variationInfo != null ? variationInfo.getBarcode() : null;
    }

    public String getImageUrl() {
        if (variationInfo != null && variationInfo.getImages() != null && !variationInfo.getImages().isEmpty()) {
            return variationInfo.getImages().get(0);
        }
        return null;
    }

    public Integer getReturnQuantity() {
        return returnQuantity;
    }

    public Integer getReturningQuantity() {
        return returningQuantity;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class VariationInfo {

        @JsonProperty("name")
        private String name;

        @JsonProperty("detail")
        private String detail;

        @JsonProperty("fields")
        private java.util.List<VariationField> fields;

        @JsonProperty("images")
        private java.util.List<String> images;

        @JsonProperty("weight")
        private Integer weight;

        @JsonProperty("barcode")
        private String barcode;

        @JsonProperty("display_id")
        private String displayId;

        @JsonProperty("retail_price")
        private Long retailPrice;

        @JsonProperty("product_display_id")
        private String productDisplayId;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class VariationField {

        @JsonProperty("id")
        private String id;

        @JsonProperty("name")
        private String name;

        @JsonProperty("value")
        private String value;

        @JsonProperty("keyValue")
        private String keyValue;
    }
}