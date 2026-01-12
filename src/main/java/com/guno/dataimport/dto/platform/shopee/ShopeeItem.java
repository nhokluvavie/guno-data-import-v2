package com.guno.dataimport.dto.platform.shopee;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * ShopeeItem - Item trong order_detail.item_list
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ShopeeItem {

    @JsonProperty("item_id")
    private Long itemId;

    @JsonProperty("item_name")
    private String itemName;

    @JsonProperty("item_sku")
    private String itemSku;

    @JsonProperty("model_id")
    private Long modelId;

    @JsonProperty("model_name")
    private String modelName;

    @JsonProperty("model_sku")
    private String modelSku;

    @JsonProperty("model_quantity_purchased")
    private Integer modelQuantityPurchased;

    @JsonProperty("model_original_price")
    private Long modelOriginalPrice;

    @JsonProperty("model_discounted_price")
    private Long modelDiscountedPrice;

    @JsonProperty("wholesale")
    private Boolean wholesale;

    @JsonProperty("weight")
    private Double weight;

    @JsonProperty("add_on_deal")
    private Boolean addOnDeal;

    @JsonProperty("main_item")
    private Boolean mainItem;

    @JsonProperty("add_on_deal_id")
    private Long addOnDealId;

    @JsonProperty("promotion_type")
    private String promotionType;

    @JsonProperty("promotion_id")
    private Long promotionId;

    @JsonProperty("promotion_group_id")
    private Long promotionGroupId;

    @JsonProperty("order_item_id")
    private Long orderItemId;

    @JsonProperty("promotion_list")
    @Builder.Default
    private List<ShopeePromotion> promotionList = new ArrayList<>();

    @JsonProperty("product_location_id")
    @Builder.Default
    private List<String> productLocationId = new ArrayList<>();

    @JsonProperty("is_prescription_item")
    private Boolean isPrescriptionItem;

    @JsonProperty("is_b2c_owned_item")
    private Boolean isB2cOwnedItem;

    @JsonProperty("hot_listing_item")
    private Boolean hotListingItem;

    @JsonProperty("consultation_id")
    private String consultationId;

    @JsonProperty("image_info")
    private ShopeeImageInfo imageInfo;

    // Helper methods
    public String getSkuOrDefault() {
        if (modelSku != null && !modelSku.isEmpty()) return modelSku;
        if (itemSku != null && !itemSku.isEmpty()) return itemSku;
        return "SHOPEE_" + itemId;
    }

    public Integer getQuantity() {
        return modelQuantityPurchased != null ? modelQuantityPurchased : 0;
    }

    public Double getOriginalPriceAsDouble() {
        return modelOriginalPrice != null ? modelOriginalPrice.doubleValue() : 0.0;
    }

    public Double getDiscountedPriceAsDouble() {
        return modelDiscountedPrice != null ? modelDiscountedPrice.doubleValue() : 0.0;
    }
}