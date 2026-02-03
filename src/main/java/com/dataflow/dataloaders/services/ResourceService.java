package com.dataflow.dataloaders.services;


import com.dataflow.dataloaders.dao.ResourceDao;
import com.dataflow.dataloaders.entity.Resource;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.Identifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.sql.DriverManager;
import java.util.*;

@Slf4j
@Service
public class ResourceService {

    protected static final String logPrefix = "{} : {}";

    // Resource type configuration definitions
    // Maps resource type to its required configuration fields
    public static final Map<String, List<ConfigField>> RESOURCE_CONFIGS = new LinkedHashMap<>();

    static {
        // SQL Server (aqstrike)
        RESOURCE_CONFIGS.put("SQL Server (aqstrike)", Arrays.asList(
                new ConfigField("jdbcUrl", "JDBC URL", true, null),
                new ConfigField("jdbcDriverName", "JDBC Driver Name", true, "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
                new ConfigField("jdbcUser", "JDBC User", true, null),
                new ConfigField("jdbcPassword", "JDBC Password", true, null, true)
        ));

        // SQL Server (abi-azure)
        RESOURCE_CONFIGS.put("SQL Server (abi-azure)", Arrays.asList(
                new ConfigField("jdbcUrl", "JDBC URL", true, null),
                new ConfigField("jdbcDriverName", "JDBC Driver Name", true, "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
                new ConfigField("jdbcUser", "JDBC User", true, null),
                new ConfigField("jdbcPassword", "JDBC Password", true, null, true)
        ));

        // Cassandra
        RESOURCE_CONFIGS.put("Cassandra", Arrays.asList(
                new ConfigField("cassandraPort", "Cassandra Port", true, null),
                new ConfigField("cassandraUser", "Cassandra User", true, null),
                new ConfigField("cassandraPassword", "Cassandra Password", true, null, true),
                new ConfigField("cassandraHostnames", "Cassandra Hostnames", true, null)
        ));

        // Azure SQL Server
        RESOURCE_CONFIGS.put("Azure SQL Server", Arrays.asList(
                new ConfigField("jdbcUrl", "JDBC URL", true, null),
                new ConfigField("jdbcDriverName", "JDBC Driver Name", true, "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
                new ConfigField("jdbcUser", "JDBC User", true, null),
                new ConfigField("jdbcPassword", "JDBC Password", true, null, true)
        ));

        // Cosmos DB
        RESOURCE_CONFIGS.put("Cosmos DB", Arrays.asList(
                new ConfigField("cassandraPort", "Cassandra Port", true, null),
                new ConfigField("cassandraUser", "Cassandra User", true, null),
                new ConfigField("cassandraPassword", "Cassandra Password", true, null, true),
                new ConfigField("cassandraHostnames", "Cassandra Hostnames", true, null)
        ));

        // MongoDB
        RESOURCE_CONFIGS.put("MongoDB", Arrays.asList(
                new ConfigField("mongoConnectionString", "MongoDB Connection String", true, null, true)
        ));

        // Oracle
        RESOURCE_CONFIGS.put("Oracle", Arrays.asList(
                new ConfigField("jdbcUrl", "JDBC URL", true, null),
                new ConfigField("jdbcDriverName", "JDBC Driver Name", true, "oracle.jdbc.driver.OracleDriver"),
                new ConfigField("jdbcUser", "JDBC User", true, null),
                new ConfigField("jdbcPassword", "JDBC Password", true, null, true)
        ));

        // PostgreSQL
        RESOURCE_CONFIGS.put("PostgreSQL", Arrays.asList(
                new ConfigField("jdbcUrl", "JDBC URL", true, null),
                new ConfigField("jdbcDriverName", "JDBC Driver Name", true, "org.postgresql.Driver"),
                new ConfigField("jdbcUser", "JDBC User", true, null),
                new ConfigField("jdbcPassword", "JDBC Password", true, null, true)
        ));

        // Redis
        RESOURCE_CONFIGS.put("Redis", Arrays.asList(
                new ConfigField("redisHostname", "Redis Hostname", true, null),
                new ConfigField("redisPort", "Redis Port", true, null),
                new ConfigField("redisPassword", "Redis Password", false, null, true),
                new ConfigField("redisDatabase", "Redis Database", false, null)
        ));

        // SFTP
        RESOURCE_CONFIGS.put("SFTP", Arrays.asList(
                new ConfigField("sftpHost", "SFTP Host", true, null),
                new ConfigField("sftpPort", "SFTP Port", true, null),
                new ConfigField("sftpUser", "SFTP User", true, null),
                new ConfigField("sftpPassword", "SFTP Password", true, null, true)
        ));

        // Solace (JMS)
        RESOURCE_CONFIGS.put("Solace (JMS)", Arrays.asList(
                new ConfigField("jmsHostname", "JMS Hostname", true, null),
                new ConfigField("jmsVpn", "JMS VPN", true, null),
                new ConfigField("jmsClientId", "JMS Client ID", true, null),
                new ConfigField("jmsUsername", "JMS Username", true, null),
                new ConfigField("jmsPassword", "JMS Password", true, null, true)
        ));

        RESOURCE_CONFIGS.put("MySQL", Arrays.asList(
                new ConfigField("jdbcUrl", "JDBC URL", true, null),
                new ConfigField("jdbcDriverName", "JDBC Driver Name", true, "org.mysql.Driver"),
                new ConfigField("jdbcUser", "JDBC User", true, null),
                new ConfigField("jdbcPassword", "JDBC Password", true, null, true)
        ));
    }

    @Autowired
    private ResourceDao resourceDao;

    @Autowired
    private  ActivityLogService activityLogService;

    @Transactional
    public Resource create(Resource resource, Identifier identifier) {
        log.info(logPrefix, this.getClass().getSimpleName(), "create - identifier: {}", identifier);
        // Validate resource type
        validateResourceType(resource.getResourceType());
        // Validate required configuration fields
        validateConfiguration(resource.getResourceType(), resource.getConfiguration());
        // Apply default values if not provided
        applyDefaultValues(resource);
        // Get the next display order
        long nextOrder = resourceDao.getMaxDisplayOrder() + 1;
        resource.setDisplayOrder(nextOrder);
        resource.setStatus("INACTIVE");
        resource.setIsActive(false);
        resource.setCreatedBy("admin");
        resource.setCreatedAt(DateUtils.getUnixTimestampInUTC());

        Resource created = resourceDao.create(resource, identifier);
        log.info("Created resource: {} of type: {} ",
                created.getId(), created.getResourceType());
        activityLogService.logResourceActivity("New Connection has been added.", created.getId(),
                created.getResourceName(), "admin");
        return maskSensitiveData(created);
    }

    public Resource getResource(Identifier identifier) {
        log.info(logPrefix, this.getClass().getSimpleName(), "getResource - identifier: {}", identifier);
        Resource resource = resourceDao.getV1(identifier)
                .orElseThrow(() -> {
                    log.warn("Resource not found for identifier: {}", identifier);
                    return new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND);
                });
        return maskSensitiveData(resource);
    }

    public List<Resource> getAllResources(Identifier identifier) {
        log.info(logPrefix, this.getClass().getSimpleName(), "getAllResources - identifier: {}", identifier);
        List<Resource> resources = resourceDao.list(identifier);
        if (resources.isEmpty()) {
            log.warn("No resources found for identifier: {}", identifier);
            return Collections.emptyList();
        }
        resources.forEach(this::maskSensitiveData);
        log.info("Found {} resources sorted by display order", resources.size());
        return resources;
    }

    public List<Resource> getResourcesByType(Identifier identifier) {
        log.info(logPrefix, this.getClass().getSimpleName(),
                "getResourcesByType - resourceType: {}", identifier.getWord());
        List<Resource> resources = resourceDao.listByResourceType(identifier.getWord());
        if (resources.isEmpty()) {
            log.warn("No resources found for resource type: {}", identifier.getWord());
            return Collections.emptyList();
        }
        resources.forEach(this::maskSensitiveData);
        log.info("Found {} resources for resource type: {}", resources.size(), identifier.getWord());
        return resources;
    }

    @Transactional
    public Object deleteResource(Identifier identifier) {
        log.info("deleteResource - identifier: {}", identifier);
        Resource resource = resourceDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));

        resourceDao.delete(resource);
        log.info("Deleted resource: {}", identifier);

        return true;
    }

    public Resource updateResource(Resource resource, Identifier identifier) {
        log.info("updateResource - identifier: {}, resource: {}", identifier, resource);
        Optional<Resource> existingRes = resourceDao.getV1(identifier);
        if (existingRes.isEmpty()) {
            log.warn("Resource not found for update - identifier: {}", identifier);
            throw new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND);
        }

        Resource res = existingRes.get();

        // Update basic fields
        if (resource.getResourceName() != null) {
            res.setResourceName(resource.getResourceName());
        }
        if (resource.getDescription() != null) {
            res.setDescription(resource.getDescription());
        }

        // Update configuration (merge with existing)
        if (resource.getConfiguration() != null && !resource.getConfiguration().isEmpty()) {
            Map<String, Object> existingConfig = res.getConfiguration() != null ?
                    new HashMap<>(res.getConfiguration()) : new HashMap<>();
            existingConfig.putAll(resource.getConfiguration());
            res.setConfiguration(existingConfig);
        }

        if (resource.getDisplayOrder() != null && resource.getDisplayOrder() != 0) {
            res.setDisplayOrder(resource.getDisplayOrder());
        }

        res.setUpdatedBy("admin");
        res.setUpdatedAt(DateUtils.getUnixTimestampInUTC());

        boolean updated = resourceDao.updateResource(res, identifier) > 0;
        if (!updated) {
            log.error("Failed to update resource: {}", res.getId());
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, "Not updated");
        }
        log.info("Updated resource: {}", res.getId());
        activityLogService.logResourceActivity("Connection has been updated.", res.getId(),
                res.getResourceName(), "admin");
        return maskSensitiveData(resourceDao.getV1(identifier).orElse(res));
    }

    public Resource updateResourceStatus(Identifier identifier, Boolean isActive) {
        log.info("updateResourceStatus - identifier: {}, isActive: {}", identifier, isActive);
        Resource resource = resourceDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));

        resource.setIsActive(isActive);
        resource.setStatus(isActive ? "ACTIVE" : "INACTIVE");
        resource.setUpdatedBy("admin");
        resource.setUpdatedAt(DateUtils.getUnixTimestampInUTC());

        resourceDao.updateResource(resource, identifier);
        log.info("Updated resource status: {} to {}", resource.getId(), isActive);
        return maskSensitiveData(resource);
    }

    @Transactional
    public List<Resource> bulkReorderResources(List<Resource> resources, Identifier identifier) {
        log.info("bulkReorderResources - updating {} resources in bulk", resources.size());
        try {
            for (int i = 0; i < resources.size(); i++) {
                Resource res = resources.get(i);
                long newOrder = i + 1;
                int updated = resourceDao.updateDisplayOrder(res.getId(), newOrder);
                if (updated <= 0) {
                    log.error("Failed to update display order for resource: {}", res.getId());
                    throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION,
                            "Failed to update display order for: " + res.getId());
                }
                log.debug("Updated resource {} with display order: {}", res.getId(), newOrder);
            }
            log.info("Successfully reordered all {} resources in bulk", resources.size());
            List<Resource> result = resourceDao.listSortedByOrder(identifier);
            result.forEach(this::maskSensitiveData);
            return result;
        } catch (Exception e) {
            log.error("Error during bulk reorder: {}", e.getMessage());
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION,
                    "Bulk reorder failed: " + e.getMessage());
        }
    }

    public Map<String, Object> testResource(Identifier identifier) {
        log.info("testResource - identifier: {}", identifier);
        Resource resource = resourceDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));

        return performResourceTest(resource);
    }

    public Map<String, Object> testNewResource(Resource resource, Identifier identifier) {
        log.info("testNewResource - resourceType: {}", resource.getResourceType());
        validateResourceType(resource.getResourceType());
        validateConfiguration(resource.getResourceType(), resource.getConfiguration());

        return performResourceTest(resource);
    }

    /**
     * Returns supported resource types with their configuration field definitions
     */
    public Map<String, List<Map<String, Object>>> getSupportedResourceTypes() {
        Map<String, List<Map<String, Object>>> result = new LinkedHashMap<>();

        for (Map.Entry<String, List<ConfigField>> entry : RESOURCE_CONFIGS.entrySet()) {
            List<Map<String, Object>> fields = new ArrayList<>();
            for (ConfigField field : entry.getValue()) {
                Map<String, Object> fieldMap = new LinkedHashMap<>();
                fieldMap.put("name", field.getName());
                fieldMap.put("label", field.getLabel());
                fieldMap.put("required", field.isRequired());
                if (field.getDefaultValue() != null) {
                    fieldMap.put("defaultValue", field.getDefaultValue());
                }
                if (field.isPassword()) {
                    fieldMap.put("type", "password");
                }
                fields.add(fieldMap);
            }
            result.put(entry.getKey(), fields);
        }

        return result;
    }

    // ==================== Private Helper Methods ====================

    private void validateResourceType(String resourceType) {
        if (resourceType == null || !RESOURCE_CONFIGS.containsKey(resourceType)) {
            log.error("Invalid resource type: {}", resourceType);
            throw new DataloadersException(ErrorFactory.VALIDATION_ERROR,
                    "Invalid resource type: " + resourceType);
        }
    }

    private void validateConfiguration(String resourceType, Map<String, Object> configuration) {
        if (configuration == null) {
            configuration = new HashMap<>();
        }

        List<ConfigField> requiredFields = RESOURCE_CONFIGS.get(resourceType);
        List<String> missingFields = new ArrayList<>();

        for (ConfigField field : requiredFields) {
            if (field.isRequired()) {
                Object value = configuration.get(field.getName());
                if (value == null || (value instanceof String && ((String) value).trim().isEmpty())) {
                    // Check if there's a default value
                    if (field.getDefaultValue() == null) {
                        missingFields.add(field.getName());
                    }
                }
            }
        }

        if (!missingFields.isEmpty()) {
            log.error("Missing required configuration fields for {}: {}", resourceType, missingFields);
            throw new DataloadersException(ErrorFactory.VALIDATION_ERROR,
                    "Missing required configuration fields: " + String.join(", ", missingFields));
        }
    }

    private void applyDefaultValues(Resource resource) {
        if (resource.getConfiguration() == null) {
            resource.setConfiguration(new HashMap<>());
        }

        List<ConfigField> fields = RESOURCE_CONFIGS.get(resource.getResourceType());
        Map<String, Object> config = resource.getConfiguration();

        for (ConfigField field : fields) {
            if (field.getDefaultValue() != null && !config.containsKey(field.getName())) {
                config.put(field.getName(), field.getDefaultValue());
            }
        }
    }

    private Resource maskSensitiveData(Resource resource) {
        if (resource.getConfiguration() == null) {
            return resource;
        }

        List<ConfigField> fields = RESOURCE_CONFIGS.get(resource.getResourceType());
        if (fields == null) {
            return resource;
        }

        Map<String, Object> maskedConfig = new HashMap<>(resource.getConfiguration());

//        for (ConfigField field : fields) {
//            if (field.isPassword() && maskedConfig.containsKey(field.getName())) {
//                Object value = maskedConfig.get(field.getName());
//                if (value != null && !value.toString().isEmpty()) {
//                    maskedConfig.put(field.getName(), "********");
//                }
//            }
//        }

        // Special handling for MongoDB connection string (mask password in URL)
        if (maskedConfig.containsKey("mongoConnectionString")) {
            String connStr = (String) maskedConfig.get("mongoConnectionString");
            if (connStr != null) {
                maskedConfig.put("mongoConnectionString",
                        connStr.replaceAll("://([^:]+):([^@]+)@", "://$1:********@"));
            }
        }

        resource.setConfiguration(maskedConfig);
        return resource;
    }

    private Map<String, Object> performResourceTest(Resource resource) {
        Map<String, Object> result = new HashMap<>();
        result.put("resourceId", resource.getId());
        result.put("resourceType", resource.getResourceType());
        result.put("testedAt", DateUtils.getUnixTimestampInUTC());

        try {
            boolean success = false;
            String message = "";
            Map<String, Object> config = resource.getConfiguration();

            switch (resource.getResourceType()) {
                case "SQL Server (aqstrike)":
                case "SQL Server (abi-azure)":
                case "Azure SQL Server":
                case "Oracle":
                case "PostgreSQL":
                    success = testJdbcConnection(config);
                    message = success ? "JDBC connection successful" : "JDBC connection failed";
                    break;

                case "Cassandra":
                case "Cosmos DB":
                    message = "Cassandra connection test not implemented";
                    break;

                case "MongoDB":
                    message = "MongoDB connection test not implemented";
                    break;

                case "Redis":
                    message = "Redis connection test not implemented";
                    break;

                case "SFTP":
                    message = "SFTP connection test not implemented";
                    break;

                case "Solace (JMS)":
                    message = "JMS connection test not implemented";
                    break;

                default:
                    message = "Unknown resource type";
            }

            result.put("success", success);
            result.put("message", message);

            if (resource.getId() != null && success) {
                resourceDao.updateResourceStatus(resource.getId(), "ACTIVE",
                        DateUtils.getUnixTimestampInUTC());
            }

        } catch (Exception e) {
            log.error("Resource test failed: {}", e.getMessage());
            result.put("success", false);
            result.put("message", "Resource test failed: " + e.getMessage());

            if (resource.getId() != null) {
                resourceDao.updateResourceStatus(resource.getId(), "FAILED",
                        DateUtils.getUnixTimestampInUTC());
            }
        }

        return result;
    }

    private boolean testJdbcConnection(Map<String, Object> config) {
        java.sql.Connection jdbcConnection = null;
        try {
            String driverName = (String) config.get("jdbcDriverName");
            String url = (String) config.get("jdbcUrl");
            String user = (String) config.get("jdbcUser");
            String password = (String) config.get("jdbcPassword");

            Class.forName(driverName);
            jdbcConnection = DriverManager.getConnection(url, user, password);
            return jdbcConnection != null && !jdbcConnection.isClosed();
        } catch (Exception e) {
            log.error("JDBC connection test failed: {}", e.getMessage());
            return false;
        } finally {
            if (jdbcConnection != null) {
                try {
                    jdbcConnection.close();
                } catch (Exception e) {
                    log.warn("Error closing test connection: {}", e.getMessage());
                }
            }
        }
    }

    public Object getResourcesByItemId(Identifier identifier) {
        log.info(logPrefix, this.getClass().getSimpleName(),
                "getResourcesByItem - ItemId: {}", identifier.getWord());
        List<Resource> resources = resourceDao.listByItemId(identifier.getWord());
        if (resources.isEmpty()) {
            log.warn("No resources found for resource type: {}", identifier.getWord());
            return Collections.emptyList();
        }
        resources.forEach(this::maskSensitiveData);
        log.info("Found {} resources for resource type: {}", resources.size(), identifier.getWord());
        return resources;
    }

    // ==================== Inner Classes ====================

    /**
     * Configuration field definition
     */
    public static class ConfigField {
        private final String name;
        private final String label;
        private final boolean required;
        private final String defaultValue;
        private final boolean password;

        public ConfigField(String name, String label, boolean required, String defaultValue) {
            this(name, label, required, defaultValue, false);
        }

        public ConfigField(String name, String label, boolean required, String defaultValue, boolean password) {
            this.name = name;
            this.label = label;
            this.required = required;
            this.defaultValue = defaultValue;
            this.password = password;
        }

        public String getName() { return name; }
        public String getLabel() { return label; }
        public boolean isRequired() { return required; }
        public String getDefaultValue() { return defaultValue; }
        public boolean isPassword() { return password; }
    }
}