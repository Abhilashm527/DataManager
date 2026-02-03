package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dao.ResourceTypeDao;
import com.dataflow.dataloaders.entity.ConfigField;
import com.dataflow.dataloaders.entity.ResourceType;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.Identifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Slf4j
@Service
public class ResourceTypeService {

    protected static final String logPrefix = "{} : {}";

    @Autowired
    private ResourceTypeDao resourceTypeDao;

    /**
     * Create a new resource type
     */
    @Transactional
    public ResourceType create(ResourceType resourceType, Identifier identifier) {
        log.info(logPrefix, this.getClass().getSimpleName(), "create - identifier: {}", identifier);

        // Validate required fields
        validateResourceType(resourceType);

        // Validate config fields
        validateConfigFields(resourceType.getConfigFields());

        // Check if type name already exists
        if (resourceTypeDao.getByTypeName(resourceType.getTypeName()).isPresent()) {
            log.error("Resource type with name '{}' already exists", resourceType.getTypeName());
            throw new DataloadersException(ErrorFactory.DUPLICATION,
                    "Resource type with name '" + resourceType.getTypeName() + "' already exists");
        }

        // Set display order
        long nextOrder = resourceTypeDao.getMaxDisplayOrder() + 1;
        resourceType.setDisplayOrder(nextOrder);

        // Set defaults
        if (resourceType.getIsActive() == null) {
            resourceType.setIsActive(true);
        }
        resourceType.setCreatedBy("admin");
        resourceType.setCreatedAt(DateUtils.getUnixTimestampInUTC());

        // Sort config fields by order
        sortConfigFields(resourceType.getConfigFields());

        ResourceType created = resourceTypeDao.create(resourceType, identifier);
        log.info("Created resource type: {} - {} at order: {}",
                created.getId(), created.getTypeName(), nextOrder);
        return created;
    }

    /**
     * Get resource type by ID
     */
    public ResourceType getResourceType(Identifier identifier) {
        log.info(logPrefix, this.getClass().getSimpleName(), "getResourceType - identifier: {}", identifier);
        ResourceType resourceType = resourceTypeDao.getV1(identifier)
                .orElseThrow(() -> {
                    log.warn("Resource type not found for identifier: {}", identifier);
                    return new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND);
                });
        return resourceType;
    }

    /**
     * Get resource type by type name
     */
    public ResourceType getResourceTypeByName(Identifier identifier, String type) {
        log.info(logPrefix, this.getClass().getSimpleName(), "getResourceTypeByName - typeName: {}", identifier.getWord());
        if( type == null ){
            return resourceTypeDao.getByTypeName(identifier.getWord())
                    .orElseThrow(() -> {
                        log.warn("Resource type not found for type name: {}", identifier.getWord());
                        return new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND);
                    });
        }
        if(type.equalsIgnoreCase("source")){
            if(identifier.getWord().equalsIgnoreCase("MySQL")||
                    identifier.getWord().equalsIgnoreCase("Oracle") ||
                    identifier.getWord().equalsIgnoreCase("PostgreSQL") ) {
                return resourceTypeDao.getByTypeName("source-jdbc")
                        .orElseThrow(() -> {
                            log.warn("Resource type not found for type name: {}", identifier.getWord());
                            return new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND);
                        });
            } else if (identifier.getWord().equalsIgnoreCase("SFTP")) {
                return resourceTypeDao.getByTypeName("source-sftp")
                        .orElseThrow(() -> {
                            log.warn("Resource type not found for type name: {}", identifier.getWord());
                            return new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND);
                        });
            }
        } else if(type.equalsIgnoreCase("target")){
            if (identifier.getWord().equalsIgnoreCase("SFTP")) {
                return resourceTypeDao.getByTypeName("target-sftp")
                        .orElseThrow(() -> {
                            log.warn("Resource type not found for type name: {}", identifier.getWord());
                            return new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND);
                        });
            }
        }

      return null;
    }

    /**
     * Get all resource types
     */
    public List<ResourceType> getAllResourceTypes(Identifier identifier) {
        log.info(logPrefix, this.getClass().getSimpleName(), "getAllResourceTypes - identifier: {}", identifier);
        List<ResourceType> resourceTypes = resourceTypeDao.list(identifier);
        if (resourceTypes.isEmpty()) {
            log.warn("No resource types found");
            return Collections.emptyList();
        }
        log.info("Found {} resource types", resourceTypes.size());
        return resourceTypes;
    }

    /**
     * Get all active resource types (for frontend)
     */
    public List<ResourceType> getActiveResourceTypes() {
        log.info(logPrefix, this.getClass().getSimpleName(), "getActiveResourceTypes");
        List<ResourceType> resourceTypes = resourceTypeDao.listActive();
        if (resourceTypes.isEmpty()) {
            log.warn("No active resource types found");
            return Collections.emptyList();
        }
        log.info("Found {} active resource types", resourceTypes.size());
        return resourceTypes;
    }

    /**
     * Update a resource type
     */
    @Transactional
    public ResourceType updateResourceType(ResourceType resourceType, Identifier identifier) {
        log.info("updateResourceType - identifier: {}, resourceType: {}", identifier, resourceType);

        Optional<ResourceType> existingOpt = resourceTypeDao.getV1(identifier);
        if (existingOpt.isEmpty()) {
            log.warn("Resource type not found for update - identifier: {}", identifier);
            throw new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND);
        }

        ResourceType existing = existingOpt.get();

        // Check if new type name conflicts with another resource type
        if (resourceType.getTypeName() != null && !resourceType.getTypeName().equals(existing.getTypeName())) {
            Optional<ResourceType> conflicting = resourceTypeDao.getByTypeName(resourceType.getTypeName());
            if (conflicting.isPresent() && !conflicting.get().getId().equals(existing.getId())) {
                log.error("Resource type with name '{}' already exists", resourceType.getTypeName());
                throw new DataloadersException(ErrorFactory.DUPLICATION,
                        "Resource type with name '" + resourceType.getTypeName() + "' already exists");
            }
        }

        // Update fields
        if (resourceType.getTypeName() != null) {
            existing.setTypeName(resourceType.getTypeName());
        }
        if (resourceType.getDescription() != null) {
            existing.setDescription(resourceType.getDescription());
        }
        if (resourceType.getConfigFields() != null) {
            validateConfigFields(resourceType.getConfigFields());
            sortConfigFields(resourceType.getConfigFields());
            existing.setConfigFields(resourceType.getConfigFields());
        }
        if (resourceType.getDisplayOrder() != null && resourceType.getDisplayOrder() != 0) {
            existing.setDisplayOrder(resourceType.getDisplayOrder());
        }
        if (resourceType.getIsActive() != null) {
            existing.setIsActive(resourceType.getIsActive());
        }

        existing.setUpdatedBy("admin");
        existing.setUpdatedAt(DateUtils.getUnixTimestampInUTC());

        boolean updated = resourceTypeDao.updateResourceType(existing, identifier) > 0;
        if (!updated) {
            log.error("Failed to update resource type: {}", existing.getId());
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, "Not updated");
        }
        log.info("Updated resource type: {}", existing.getId());
        return resourceTypeDao.getV1(identifier).orElse(existing);
    }

    /**
     * Delete a resource type (soft delete)
     */
    @Transactional
    public Object deleteResourceType(Identifier identifier) {
        log.info("deleteResourceType - identifier: {}", identifier);

        ResourceType resourceType = resourceTypeDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));

        // TODO: Check if any resources are using this type before deleting
        // You may want to prevent deletion if resources exist

        resourceTypeDao.delete(resourceType);
        log.info("Deleted resource type: {}", identifier);
        return true;
    }

    /**
     * Update resource type status (activate/deactivate)
     */
    @Transactional
    public ResourceType updateResourceTypeStatus(Identifier identifier, Boolean isActive) {
        log.info("updateResourceTypeStatus - identifier: {}, isActive: {}", identifier, isActive);

        ResourceType resourceType = resourceTypeDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));

        resourceType.setIsActive(isActive);
        resourceType.setUpdatedBy("admin");
        resourceType.setUpdatedAt(DateUtils.getUnixTimestampInUTC());

        resourceTypeDao.updateResourceType(resourceType, identifier);
        log.info("Updated resource type status: {} to {}", resourceType.getId(), isActive);
        return resourceType;
    }

    /**
     * Bulk reorder resource types
     */
    @Transactional
    public List<ResourceType> bulkReorderResourceTypes(List<ResourceType> resourceTypes, Identifier identifier) {
        log.info("bulkReorderResourceTypes - updating {} resource types in bulk", resourceTypes.size());
        try {
            for (int i = 0; i < resourceTypes.size(); i++) {
                ResourceType rt = resourceTypes.get(i);
                long newOrder = i + 1;

                ResourceType existing = resourceTypeDao.getV1(new Identifier(rt.getId()))
                        .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND,
                                "Resource type not found: " + rt.getId()));

                existing.setDisplayOrder(newOrder);
                existing.setUpdatedBy("admin");
                existing.setUpdatedAt(DateUtils.getUnixTimestampInUTC());

                int updated = resourceTypeDao.updateResourceType(existing, new Identifier(rt.getId()));
                if (updated <= 0) {
                    log.error("Failed to update display order for resource type: {}", rt.getId());
                    throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION,
                            "Failed to update display order for: " + rt.getId());
                }
                log.debug("Updated resource type {} with display order: {}", rt.getId(), newOrder);
            }
            log.info("Successfully reordered all {} resource types in bulk", resourceTypes.size());
            return resourceTypeDao.list(identifier);
        } catch (DataloadersException e) {
            throw e;
        } catch (Exception e) {
            log.error("Error during bulk reorder: {}", e.getMessage());
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION,
                    "Bulk reorder failed: " + e.getMessage());
        }
    }

    // ==================== Private Helper Methods ====================

    /**
     * Validate resource type required fields
     */
    private void validateResourceType(ResourceType resourceType) {
        List<String> missingFields = new ArrayList<>();

        if (resourceType.getTypeName() == null || resourceType.getTypeName().trim().isEmpty()) {
            missingFields.add("typeName");
        }
        if (resourceType.getConfigFields() == null || resourceType.getConfigFields().isEmpty()) {
            missingFields.add("configFields");
        }

        if (!missingFields.isEmpty()) {
            log.error("Missing required fields: {}", missingFields);
            throw new DataloadersException(ErrorFactory.VALIDATION_ERROR,
                    "Missing required fields: " + String.join(", ", missingFields));
        }
    }

    /**
     * Validate config fields structure
     */
    private void validateConfigFields(List<ConfigField> configFields) {
        if (configFields == null || configFields.isEmpty()) {
            return;
        }

        Set<String> fieldNames = new HashSet<>();
        for (int i = 0; i < configFields.size(); i++) {
            ConfigField field = configFields.get(i);
            List<String> errors = new ArrayList<>();

            // Validate required properties
            if (field.getName() == null || field.getName().trim().isEmpty()) {
                errors.add("name is required");
            } else if (fieldNames.contains(field.getName())) {
                errors.add("duplicate field name: " + field.getName());
            } else {
                fieldNames.add(field.getName());
            }

            if (field.getLabel() == null || field.getLabel().trim().isEmpty()) {
                errors.add("label is required");
            }

            if (field.getType() == null || field.getType().trim().isEmpty()) {
                errors.add("type is required");
            } else {
                // Validate type value
                List<String> validTypes = Arrays.asList("text", "password", "number", "boolean", "textarea", "select");
                if (!validTypes.contains(field.getType())) {
                    errors.add("invalid type: " + field.getType() + ". Valid types: " + validTypes);
                }
            }

            if (field.getRequired() == null) {
                // Default to false
                field.setRequired(false);
            }

            // Set default order if not provided
            if (field.getOrder() == null) {
                field.setOrder(i + 1);
            }

            if (!errors.isEmpty()) {
                log.error("Validation errors for configField[{}]: {}", i, errors);
                throw new DataloadersException(ErrorFactory.VALIDATION_ERROR,
                        "ConfigField[" + i + "] errors: " + String.join(", ", errors));
            }
        }
    }

    /**
     * Sort config fields by order
     */
    private void sortConfigFields(List<ConfigField> configFields) {
        if (configFields != null && !configFields.isEmpty()) {
            configFields.sort(Comparator.comparingInt(f -> f.getOrder() != null ? f.getOrder() : Integer.MAX_VALUE));
        }
    }
}