package com.guno.dataimport.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * PartnerStatus Entity - Maps to tbl_partner_status table
 * UPDATED: id changed from String to Integer for better performance
 *
 * Schema: (id, partner_status_name, stage)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PartnerStatus {

    /**
     * Primary key - Integer ID (0-9)
     * 0 = Unknown, 1 = Pending, 2 = Picking Up, 3 = Picked Up,
     * 4 = On Delivery, 5 = Delivered, 6 = Undeliverable,
     * 7 = Returning, 8 = Returned, 9 = Cancelled
     */
    private Integer id;  // CHANGED: String → Integer

    /**
     * Display name for partner status
     * e.g. "Picking Up", "Delivered"
     */
    private String partnerStatusName;

    /**
     * Stage category
     * e.g. "Processing", "Picking", "Shipping", "Completed", "Return", "Failed", "Cancelled"
     */
    private String stage;
}