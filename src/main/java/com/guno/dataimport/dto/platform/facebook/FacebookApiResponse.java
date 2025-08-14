package com.guno.dataimport.dto.platform.facebook;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.ArrayList;
import java.util.List;

/**
 * Facebook API Response - Wrapper for Facebook order data
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FacebookApiResponse {

    @JsonProperty("status")
    private Integer status;

    @JsonProperty("message")
    private String message;

    @JsonProperty("code")
    private Integer code;

    @JsonProperty("data")
    private FacebookDataWrapper data;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class FacebookDataWrapper {

        @JsonProperty("orders")
        @Builder.Default
        private List<FacebookOrderDto> orders = new ArrayList<>();

        @JsonProperty("count")
        private Integer count;

        @JsonProperty("page")
        private Integer page;

        @JsonProperty("total_pages")
        private Integer totalPages;

        @JsonProperty("has_next")
        private Boolean hasNext;
    }

    // Helper methods
    public boolean isSuccess() {
        return status != null && status == 200;
    }

    public int getOrderCount() {
        return data != null && data.orders != null ? data.orders.size() : 0;
    }
}