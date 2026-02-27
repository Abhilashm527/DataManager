package com.dataflow.dataloaders.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@lombok.EqualsAndHashCode(callSuper = true)
public class ActivityDefinition extends AuditMetaData {

    private String id;

    // e.g., "JDBC_READER", "JDBC_WRITER", "TRANSFORMER"
    private String activityType;

    // e.g., "READER", "WRITER", "PROCESSOR", "CONTROL"
    private String category;

    // e.g., "Relational Database Reader"
    private String label;

    private String description;

    // SVG string, or identifier of an icon
    private String iconStr;

    // List of Provider/Connection types that can be selected for this node
    private List<String> supportedConnectionTypes;

    // The exact schema defining what config fields the frontend should render
    private ConfigSchema configSchema;

    // UI triggers/buttons to invoke backend APIs like 'Preview Data' or 'View DDL'
    private List<ActivityAction> activityActions;

    @com.fasterxml.jackson.annotation.JsonIgnore
    private Long total;
}
