package com.dataflow.dataloaders.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Resource {

    private String id;                              // Resource ID: RES-000001
    private String resourceType;                    // e.g., "SQL Server (aqstrike)", "MongoDB", "Redis", etc.
    private String resourceName;                    // User-friendly name for the resource
    private String description;                     // Optional description

    /**
     * Configuration stored as JSONB - contains all resource-specific fields
     *
     * Example configurations by resource type:
     *
     * SQL Server / Azure SQL / Oracle / PostgreSQL:
     * {
     *   "jdbcUrl": "jdbc:sqlserver://localhost:1433;databaseName=mydb",
     *   "jdbcDriverName": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
     *   "jdbcUser": "sa",
     *   "jdbcPassword": "password123"
     * }
     *
     * Cassandra / Cosmos DB:
     * {
     *   "cassandraPort": 9042,
     *   "cassandraUser": "cassandra",
     *   "cassandraPassword": "password123",
     *   "cassandraHostnames": "host1,host2,host3"
     * }
     *
     * MongoDB:
     * {
     *   "mongoConnectionString": "mongodb://user:password@localhost:27017/mydb"
     * }
     *
     * Redis:
     * {
     *   "redisHostname": "localhost",
     *   "redisPort": 6379,
     *   "redisPassword": "password123",
     *   "redisDatabase": 0
     * }
     *
     * SFTP:
     * {
     *   "sftpHost": "sftp.example.com",
     *   "sftpPort": 22,
     *   "sftpUser": "user",
     *   "sftpPassword": "password123"
     * }
     *
     * Solace (JMS):
     * {
     *   "jmsHostname": "localhost",
     *   "jmsVpn": "default",
     *   "jmsClientId": "client-1",
     *   "jmsUsername": "admin",
     *   "jmsPassword": "password123"
     * }
     */
    private Map<String, Object> configuration;

    // Resource status
    private String status;                          // ACTIVE, INACTIVE, FAILED
    private Long lastTestedAt;                      // Last connection test timestamp
    private Boolean isActive;

    // Display order for UI
    private Long displayOrder;
    private String itemId;

    // Audit fields
    private String createdBy;
    private Long createdAt;
    private String updatedBy;
    private Long updatedAt;
    private Long deletedAt;
}