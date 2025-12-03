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
 * TikTok Recipient Address DTO - Shipping address from TikTok API
 * Maps to "recipient_address" object in TikTok order JSON
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TikTokRecipientAddress {

    @JsonProperty("name")
    private String name;

    @JsonProperty("first_name")
    private String firstName;

    @JsonProperty("last_name")
    private String lastName;

    @JsonProperty("phone_number")
    private String phoneNumber;

    @JsonProperty("full_address")
    private String fullAddress;

    @JsonProperty("address_line1")
    private String addressLine1;

    @JsonProperty("address_line2")
    private String addressLine2;

    @JsonProperty("address_line3")
    private String addressLine3;

    @JsonProperty("address_line4")
    private String addressLine4;

    @JsonProperty("address_detail")
    private String addressDetail;

    @JsonProperty("postal_code")
    private String postalCode;

    @JsonProperty("region_code")
    private String regionCode;

    @JsonProperty("district_info")
    @Builder.Default
    private List<DistrictInfo> districtInfo = new ArrayList<>();

    @JsonProperty("first_name_local_script")
    private String firstNameLocalScript;

    @JsonProperty("last_name_local_script")
    private String lastNameLocalScript;

    /**
     * District Info - Nested structure for administrative levels
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DistrictInfo {

        @JsonProperty("address_name")
        private String addressName;

        @JsonProperty("address_level")
        private String addressLevel;

        @JsonProperty("address_level_name")
        private String addressLevelName;
    }

    // Helper methods for easier data access

    public String getFullName() {
        if (name != null && !name.isEmpty()) {
            return name;
        }
        if (firstName != null || lastName != null) {
            return (firstName != null ? firstName : "") + " " + (lastName != null ? lastName : "");
        }
        return "";
    }

    public String getProvince() {
        return getDistrictInfoByLevel("L1");
    }

    public String getDistrict() {
        return getDistrictInfoByLevel("L2");
    }

    public String getWard() {
        return getDistrictInfoByLevel("L3");
    }

    public String getCountry() {
        return getDistrictInfoByLevel("L0");
    }

    private String getDistrictInfoByLevel(String level) {
        if (districtInfo == null) return null;
        return districtInfo.stream()
                .filter(info -> level.equals(info.getAddressLevel()))
                .map(DistrictInfo::getAddressName)
                .findFirst()
                .orElse(null);
    }

    public String getDetailAddress() {
        return addressDetail != null ? addressDetail : addressLine1;
    }
}