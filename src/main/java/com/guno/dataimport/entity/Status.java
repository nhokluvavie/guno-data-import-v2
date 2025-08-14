package com.guno.dataimport.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Status Entity - Maps to tbl_status table
 * Master table for order statuses across platforms
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Status {

    private Long statusKey;
    private String platform;

    // Platform-specific status
    private String platformStatusCode;
    private String platformStatusName;

    // Standardized status
    private String standardStatusCode;
    private String standardStatusName;
    private String statusCategory;
}