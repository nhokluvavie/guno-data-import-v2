package com.guno.dataimport.dto.platform.facebook;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ChangedLog - Lịch sử thay đổi trạng thái đơn hàng
 * Mapping từ field "histories" trong JSON
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ChangedLog {

    @JsonProperty("status")
    private ValueChange<Integer> status;

    @JsonProperty("shopee_status")
    private ValueChange<String> shopeeStatus;

    @JsonProperty("cod")
    private ValueChange<Double> cod;

    @JsonProperty("return_fee")
    private ValueChange<Boolean> returnFee;

    @JsonProperty("note")
    private JsonNode note;  // Flexible: có thể là String hoặc Object

    @JsonProperty("shipping_fee")
    private ValueChange<Double> shippingFee;

    @JsonProperty("editor_id")
    private String editorId;

    @JsonProperty("updated_at")
    private String updatedAt;

    // Helper method để lấy note value
    public String getNoteValue() {
        if (note == null) return null;
        if (note.isTextual()) return note.asText();
        if (note.isObject() && note.has("new")) {
            JsonNode newVal = note.get("new");
            return newVal.isNull() ? null : newVal.asText();
        }
        return null;
    }

    /**
     * ValueChange - Wrapper cho old/new values
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ValueChange<T> {
        @JsonProperty("old")
        private T oldValue;

        @JsonProperty("new")
        private T newValue;
    }
}