package com.dataflow.dataloaders.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Defines a configuration field for a ResourceType.
 *
 * This is used to define what fields are required/available
 * when creating a Resource of a specific ResourceType.
 *
 * Example:
 * {
 * "name": "jdbcUrl",
 * "label": "JDBC URL",
 * "required": true,
 * "defaultValue": null,
 * "type": "text",
 * "placeholder": "jdbc:sqlserver://host:port;databaseName=db",
 * "helpText": "The JDBC connection URL for the database",
 * "order": 1
 * }
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConfigField {

    /**
     * Field name - used as the key in Resource.configuration
     * Example: "jdbcUrl", "redisHostname", "cassandraPort"
     */
    private String name;

    /**
     * Display label for UI
     * Example: "JDBC URL", "Redis Hostname", "Cassandra Port"
     */
    private String label;

    /**
     * Whether this field is required
     */
    private Boolean required;

    /**
     * Default value for the field (can be null)
     * Example: "com.microsoft.sqlserver.jdbc.SQLServerDriver" for jdbcDriverName
     */
    private Object defaultValue;

    /**
     * Field type for UI rendering and validation
     * Values: "text", "password", "number", "boolean", "textarea", "select"
     */
    private String type;

    /**
     * Placeholder text for UI input
     * Example: "jdbc:sqlserver://localhost:1433;databaseName=mydb"
     */
    private String placeholder;

    /**
     * Help text or tooltip for the field
     * Example: "Enter the full JDBC connection URL"
     */
    private String helpText;

    /**
     * Display order in the form
     */
    private Integer order;

    /**
     * Options for select type fields (JSON array as string)
     * Example: ["option1", "option2", "option3"]
     */
    private String options;

    /**
     * Minimum value (for number type)
     */
    private Integer min;

    /**
     * Maximum value (for number type)
     */
    private Integer max;

    /**
     * Validation pattern (regex)
     * Example: "^jdbc:(sqlserver|postgresql|oracle):.*"
     */
    private String pattern;

    /**
     * Determines which other fields must be filled out before this field is enabled
     * Example: ["connectionId", "schemaName"]
     */
    private java.util.List<String> dependsOn;

    /**
     * For dynamic fields (e.g., dynamic_select), the API endpoint to fetch data
     * from
     * Example: "/api/v1/dag-activity/jdbc/tables?connectionId={{connectionId}}"
     */
    private String endpoint;
}