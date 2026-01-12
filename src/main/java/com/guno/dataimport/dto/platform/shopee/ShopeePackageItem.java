package com.guno.dataimport.dto.platform.shopee;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ShopeePackageItem - Item trong package.item_list
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ShopeePackageItem {

    @JsonProperty("item_id")
    private Long itemId;

    @JsonProperty("model_id")
    private Long modelId;

    @JsonProperty("order_item_id")
    private Long orderItemId;

    @JsonProperty("model_quantity")
    private Integer modelQuantity;

    @JsonProperty("product_location_id")
    private String productLocationId;

    @JsonProperty("promotion_group_id")
    private Long promotionGroupId;
}