package com.guno.dataimport.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * SubStatus Entity - Maps to tbl_substatus table
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SubStatus {
    private String id;              // sub_status_id (PK)
    private String subStatusName;   // sub_status_name
}