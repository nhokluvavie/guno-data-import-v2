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
 * Shopee API Response - Wrapper for Shopee API response
 * Pattern: Identical to TikTokApiResponse for consistency
 *
 * Location: src/main/java/com/guno/dataimport/dto/platform/shopee/ShopeeApiResponse.java
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ShopeeApiResponse {

    @JsonProperty("status")
    private Integer status;

    @JsonProperty("message")
    private String message;

    @JsonProperty("code")
    private Integer code;

    @JsonProperty("data")
    private ShopeeDataWrapper data;

    /**
     * Shopee Data Wrapper - Contains orders list and pagination info
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ShopeeDataWrapper {

        @JsonProperty("orders")
        @Builder.Default
        private List<ShopeeOrderDto> orders = new ArrayList<>();

        @JsonProperty("count")
        private Integer count;

        @JsonProperty("page")
        private Integer page;

        @JsonProperty("total_pages")
        private Integer totalPages;

        @JsonProperty("has_next")
        private Boolean hasNext;
    }

    // ================================
    // HELPER METHODS
    // ================================

    public boolean isSuccess() {
        return code != null && code == 200;
    }

    public int getOrderCount() {
        return data != null && data.orders != null ? data.orders.size() : 0;
    }

    public List<ShopeeOrderDto> getOrders() {
        return data != null && data.orders != null ? data.orders : new ArrayList<>();
    }

    public boolean hasOrders() {
        return getOrderCount() > 0;
    }

    public Boolean hasNextPage() {
        if (data == null) return null;
        return data.hasNext;
    }

    public Integer getCurrentPage() {
        return data != null ? data.page : null;
    }

    public Integer getTotalPages() {
        return data != null ? data.totalPages : null;
    }
}