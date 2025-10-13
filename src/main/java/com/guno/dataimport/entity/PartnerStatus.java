package com.guno.dataimport.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * PartnerStatus Entity - Maps to tbl_partner_status table
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PartnerStatus {
    private String id;                     // partner_status_id (PK)
    private String partnerStatusName;      // partner_status_name
    @Builder.Default
    private Boolean isReturned = false;    // is_returned
}