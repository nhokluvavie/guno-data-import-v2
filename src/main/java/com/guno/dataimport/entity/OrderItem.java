package com.guno.dataimport.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * OrderItem Entity - Maps to tbl_order_item table
 * Uses composite primary key (orderId, sku, platformProductId)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderItem {

    // Composite Primary Key
    private String orderId;
    private String sku;
    private String platformProductId;

    @Builder.Default private Integer quantity = 0;
    @Builder.Default private Double unitPrice = 0.0;
    @Builder.Default private Double totalPrice = 0.0;
    @Builder.Default private Double itemDiscount = 0.0;

    @Builder.Default private String promotionType = "";
    @Builder.Default private String promotionCode = "";
    @Builder.Default private String itemStatus = "";

    @Builder.Default private Integer itemSequence = 1;
    @Builder.Default private Long opId = 0L;
}