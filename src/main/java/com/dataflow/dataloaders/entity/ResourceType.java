package com.dataflow.dataloaders.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * ResourceType entity - Defines a type of resource and its configuration fields.
 *
 * This is the metadata/template that tells the frontend what fields are needed
 * when creating a Resource of this type.
 *
 * Example:
 * {
 *   "id": "RT-000001",
 *   "typeName": "SQL Server (aqstrike)",
 *   "description": "SQL Server connection for aqstrike environment",
 *   "configFields": [
 *     {"name": "jdbcUrl", "label": "JDBC URL", "required": true, "type": "text"},
 *     {"name": "jdbcDriverName", "label": "JDBC Driver Name", "required": true, "defaultValue": "com.microsoft.sqlserver.jdbc.SQLServerDriver", "type": "text"},
 *     {"name": "jdbcUser", "label": "JDBC User", "required": true, "type": "text"},
 *     {"name": "jdbcPassword", "label": "JDBC Password", "required": true, "type": "password"}
 *   ],
 *   "displayOrder": 1,
 *   "isActive": true
 * }
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ResourceType {

    /**
     * Unique identifier for the resource type
     * Format: RT-000001, RT-000002, etc.
     */
    private String id;

    /**
     * Name of the resource type
     * Example: "SQL Server (aqstrike)", "MongoDB", "Redis", "SFTP"
     */
    private String typeName;

    /**
     * Description of the resource type
     * Example: "SQL Server connection for aqstrike environment"
     */
    private String description;

    /**
     * List of configuration fields that define what inputs are needed
     * when creating a Resource of this type.
     * Stored as JSONB in database.
     */
    private List<ConfigField> configFields;

    /**
     * Display order for UI listing
     */
    private Long displayOrder;

    /**
     * Whether this resource type is available for use
     */
    private Boolean isActive;

    // Audit fields
    private String createdBy;
    private Long createdAt;
    private String updatedBy;
    private Long updatedAt;
    private Long deletedAt;
}