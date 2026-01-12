package com.guno.dataimport.dto.platform.shopee;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ShopeePromotion - Promotion info trong item
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ShopeePromotion {

    @JsonProperty("promotion_id")
    private Long promotionId;

    @JsonProperty("promotion_type")
    private String promotionType;
}