package com.guno.dataimport.dto.platform.tiktok;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * TikTok API Response - Wrapper for TikTok API response
 * Pattern: Similar to FacebookApiResponse
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TikTokApiResponse {

    @JsonProperty("status")
    private Integer status;

    @JsonProperty("message")
    private String message;

    @JsonProperty("code")
    private Integer code;

    @JsonProperty("data")
    private TikTokDataWrapper data;

    /**
     * TikTok Data Wrapper - Contains orders list and pagination info
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TikTokDataWrapper {

        @JsonProperty("orders")
        @Builder.Default
        private List<TikTokOrderDto> orders = new ArrayList<>();

        @JsonProperty("count")
        private Integer count;

        @JsonProperty("page")
        private Integer page;

        @JsonProperty("total_pages")
        private Integer totalPages;

        @JsonProperty("has_next")
        private Boolean hasNext;
    }

    // Helper methods for easier data access

    public boolean isSuccess() {
        return code != null && code == 200;
    }

    public int getOrderCount() {
        return data != null && data.orders != null ? data.orders.size() : 0;
    }

    public List<TikTokOrderDto> getOrders() {
        return data != null && data.orders != null ? data.orders : new ArrayList<>();
    }

    public boolean hasOrders() {
        return getOrderCount() > 0;
    }

    public boolean hasNextPage() {
        return data != null && data.hasNext != null && data.hasNext;
    }

    public Integer getCurrentPage() {
        return data != null ? data.page : null;
    }

    public Integer getTotalPages() {
        return data != null ? data.totalPages : null;
    }
}