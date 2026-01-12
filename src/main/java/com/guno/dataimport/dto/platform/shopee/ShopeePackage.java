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
 * ShopeePackage - Package trong order_detail.package_list
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ShopeePackage {

    @JsonProperty("package_number")
    private String packageNumber;

    @JsonProperty("logistics_status")
    private String logisticsStatus;

    @JsonProperty("shipping_carrier")
    private String shippingCarrier;

    @JsonProperty("item_list")
    @Builder.Default
    private List<ShopeePackageItem> itemList = new ArrayList<>();

    @JsonProperty("parcel_chargeable_weight_gram")
    private Integer parcelChargeableWeightGram;

    @JsonProperty("logistics_channel_id")
    private Integer logisticsChannelId;

    @JsonProperty("sorting_group")
    private String sortingGroup;

    @JsonProperty("group_shipment_id")
    private String groupShipmentId;

    @JsonProperty("allow_self_design_awb")
    private Boolean allowSelfDesignAwb;
}