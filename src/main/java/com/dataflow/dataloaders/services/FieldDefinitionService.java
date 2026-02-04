package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dao.FieldDefinitionDao;
import com.dataflow.dataloaders.entity.ConfigField;
import com.dataflow.dataloaders.entity.FieldDefinition;
import com.dataflow.dataloaders.enums.Provider;
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
public class FieldDefinitionService {

    protected static final String logPrefix = "{} : {}";

    @Autowired
    private FieldDefinitionDao fieldDefinitionDao;

    @Transactional
    public FieldDefinition create(FieldDefinition fieldDefinition, Identifier identifier) {
        log.info(logPrefix, this.getClass().getSimpleName(), "create - identifier: {}", identifier);

        validateFieldDefinition(fieldDefinition);
        validateConfigFields(fieldDefinition.getConfigFields());

        if (fieldDefinitionDao.getByTypeNameAndProvider(fieldDefinition.getTypeName(), fieldDefinition.getProvider())
                .isPresent()) {
            log.error("Field definition with name '{}' and provider '{}' already exists",
                    fieldDefinition.getTypeName(), fieldDefinition.getProvider());
            throw new DataloadersException(ErrorFactory.DUPLICATION,
                    "Field definition with name '" + fieldDefinition.getTypeName() +
                            "' and provider '" + fieldDefinition.getProvider() + "' already exists");
        }

        long nextOrder = fieldDefinitionDao.getMaxDisplayOrder() + 1;
        fieldDefinition.setDisplayOrder(nextOrder);

        if (fieldDefinition.getIsActive() == null) {
            fieldDefinition.setIsActive(true);
        }
        fieldDefinition.setCreatedBy("admin");
        fieldDefinition.setCreatedAt(DateUtils.getUnixTimestampInUTC());

        sortConfigFields(fieldDefinition.getConfigFields());

        FieldDefinition created = fieldDefinitionDao.create(fieldDefinition, identifier);
        log.info("Created field definition: {} - {} at order: {}",
                created.getId(), created.getTypeName(), nextOrder);
        return created;
    }

    public FieldDefinition getFieldDefinition(Identifier identifier) {
        log.info(logPrefix, this.getClass().getSimpleName(), "getFieldDefinition - identifier: {}", identifier);
        FieldDefinition fieldDefinition = fieldDefinitionDao.getV1(identifier)
                .orElseThrow(() -> {
                    log.warn("Field definition not found for identifier: {}", identifier);
                    return new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND);
                });
        return fieldDefinition;
    }

    public List<FieldDefinition> getByProvider(Provider provider) {
        log.info(logPrefix, this.getClass().getSimpleName(), "getByProvider - provider: {}", provider);
        List<FieldDefinition> fieldDefinitions = fieldDefinitionDao.getByProvider(provider);
        if (fieldDefinitions.isEmpty()) {
            log.warn("No field definitions found for provider: {}", provider);
            return Collections.emptyList();
        }
        log.info("Found {} field definitions for provider: {}", fieldDefinitions.size(), provider);
        return fieldDefinitions;
    }

    public FieldDefinition getByTypeName(String typeName) {
        log.info(logPrefix, this.getClass().getSimpleName(), "getByTypeName - typeName: {}", typeName);
        return fieldDefinitionDao.getByTypeName(typeName)
                .orElseThrow(() -> {
                    log.warn("Field definition not found for type name: {}", typeName);
                    return new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND);
                });
    }

    public List<FieldDefinition> getAllFieldDefinitions(Identifier identifier) {
        log.info(logPrefix, this.getClass().getSimpleName(), "getAllFieldDefinitions - identifier: {}", identifier);
        List<FieldDefinition> fieldDefinitions = fieldDefinitionDao.list(identifier);
        if (fieldDefinitions.isEmpty()) {
            log.warn("No field definitions found");
            return Collections.emptyList();
        }
        log.info("Found {} field definitions", fieldDefinitions.size());
        return fieldDefinitions;
    }

    public List<FieldDefinition> getActiveFieldDefinitions() {
        log.info(logPrefix, this.getClass().getSimpleName(), "getActiveFieldDefinitions");
        List<FieldDefinition> fieldDefinitions = fieldDefinitionDao.listActive();
        if (fieldDefinitions.isEmpty()) {
            log.warn("No active field definitions found");
            return Collections.emptyList();
        }
        log.info("Found {} active field definitions", fieldDefinitions.size());
        return fieldDefinitions;
    }

    @Transactional
    public FieldDefinition updateFieldDefinition(FieldDefinition fieldDefinition, Identifier identifier) {
        log.info("updateFieldDefinition - identifier: {}, fieldDefinition: {}", identifier, fieldDefinition);

        Optional<FieldDefinition> existingOpt = fieldDefinitionDao.getV1(identifier);
        if (existingOpt.isEmpty()) {
            log.warn("Field definition not found for update - identifier: {}", identifier);
            throw new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND);
        }

        FieldDefinition existing = existingOpt.get();

        if ((fieldDefinition.getTypeName() != null && !fieldDefinition.getTypeName().equals(existing.getTypeName())) ||
                (fieldDefinition.getProvider() != null
                        && !fieldDefinition.getProvider().equals(existing.getProvider()))) {

            String targetTypeName = fieldDefinition.getTypeName() != null ? fieldDefinition.getTypeName()
                    : existing.getTypeName();
            Provider targetProvider = fieldDefinition.getProvider() != null ? fieldDefinition.getProvider()
                    : existing.getProvider();

            Optional<FieldDefinition> conflicting = fieldDefinitionDao.getByTypeNameAndProvider(targetTypeName,
                    targetProvider);
            if (conflicting.isPresent() && !conflicting.get().getId().equals(existing.getId())) {
                log.error("Field definition with name '{}' and provider '{}' already exists", targetTypeName,
                        targetProvider);
                throw new DataloadersException(ErrorFactory.DUPLICATION,
                        "Field definition with name '" + targetTypeName +
                                "' and provider '" + targetProvider + "' already exists");
            }
        }

        if (fieldDefinition.getTypeName() != null) {
            existing.setTypeName(fieldDefinition.getTypeName());
        }
        if (fieldDefinition.getProvider() != null) {
            existing.setProvider(fieldDefinition.getProvider());
        }
        if (fieldDefinition.getDescription() != null) {
            existing.setDescription(fieldDefinition.getDescription());
        }
        if (fieldDefinition.getConfigFields() != null) {
            validateConfigFields(fieldDefinition.getConfigFields());
            sortConfigFields(fieldDefinition.getConfigFields());
            existing.setConfigFields(fieldDefinition.getConfigFields());
        }
        if (fieldDefinition.getDisplayOrder() != null && fieldDefinition.getDisplayOrder() != 0) {
            existing.setDisplayOrder(fieldDefinition.getDisplayOrder());
        }
        if (fieldDefinition.getIsActive() != null) {
            existing.setIsActive(fieldDefinition.getIsActive());
        }

        existing.setUpdatedBy("admin");
        existing.setUpdatedAt(DateUtils.getUnixTimestampInUTC());

        boolean updated = fieldDefinitionDao.updateResourceType(existing, identifier) > 0;
        if (!updated) {
            log.error("Failed to update field definition: {}", existing.getId());
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, "Not updated");
        }
        log.info("Updated field definition: {}", existing.getId());
        return fieldDefinitionDao.getV1(identifier).orElse(existing);
    }

    @Transactional
    public Object deleteFieldDefinition(Identifier identifier) {
        log.info("deleteFieldDefinition - identifier: {}", identifier);

        FieldDefinition fieldDefinition = fieldDefinitionDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));

        fieldDefinitionDao.delete(fieldDefinition);
        log.info("Deleted field definition: {}", identifier);
        return true;
    }

    @Transactional
    public FieldDefinition updateFieldDefinitionStatus(Identifier identifier, Boolean isActive) {
        log.info("updateFieldDefinitionStatus - identifier: {}, isActive: {}", identifier, isActive);

        FieldDefinition fieldDefinition = fieldDefinitionDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));

        fieldDefinition.setIsActive(isActive);
        fieldDefinition.setUpdatedBy("admin");
        fieldDefinition.setUpdatedAt(DateUtils.getUnixTimestampInUTC());

        fieldDefinitionDao.updateResourceType(fieldDefinition, identifier);
        log.info("Updated field definition status: {} to {}", fieldDefinition.getId(), isActive);
        return fieldDefinition;
    }

    @Transactional
    public List<FieldDefinition> bulkReorderFieldDefinitions(List<FieldDefinition> fieldDefinitions,
            Identifier identifier) {
        log.info("bulkReorderFieldDefinitions - updating {} field definitions in bulk", fieldDefinitions.size());
        try {
            for (int i = 0; i < fieldDefinitions.size(); i++) {
                FieldDefinition fd = fieldDefinitions.get(i);
                long newOrder = i + 1;

                FieldDefinition existing = fieldDefinitionDao.getV1(new Identifier(fd.getId()))
                        .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND,
                                "Field definition not found: " + fd.getId()));

                existing.setDisplayOrder(newOrder);
                existing.setUpdatedBy("admin");
                existing.setUpdatedAt(DateUtils.getUnixTimestampInUTC());

                int updated = fieldDefinitionDao.updateResourceType(existing, new Identifier(fd.getId()));
                if (updated <= 0) {
                    log.error("Failed to update display order for field definition: {}", fd.getId());
                    throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION,
                            "Failed to update display order for: " + fd.getId());
                }
                log.debug("Updated field definition {} with display order: {}", fd.getId(), newOrder);
            }
            log.info("Successfully reordered all {} field definitions in bulk", fieldDefinitions.size());
            return fieldDefinitionDao.list(identifier);
        } catch (DataloadersException e) {
            throw e;
        } catch (Exception e) {
            log.error("Error during bulk reorder: {}", e.getMessage());
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION,
                    "Bulk reorder failed: " + e.getMessage());
        }
    }

    private void validateFieldDefinition(FieldDefinition fieldDefinition) {
        List<String> missingFields = new ArrayList<>();

        if (fieldDefinition.getTypeName() == null || fieldDefinition.getTypeName().trim().isEmpty()) {
            missingFields.add("typeName");
        }
        if (fieldDefinition.getProvider() == null) {
            missingFields.add("provider");
        }
        if (fieldDefinition.getConfigFields() == null || fieldDefinition.getConfigFields().isEmpty()) {
            missingFields.add("configFields");
        }

        if (!missingFields.isEmpty()) {
            log.error("Missing required fields: {}", missingFields);
            throw new DataloadersException(ErrorFactory.VALIDATION_ERROR,
                    "Missing required fields: " + String.join(", ", missingFields));
        }
    }

    private void validateConfigFields(List<ConfigField> configFields) {
        if (configFields == null || configFields.isEmpty()) {
            return;
        }

        Set<String> fieldNames = new HashSet<>();
        for (int i = 0; i < configFields.size(); i++) {
            ConfigField field = configFields.get(i);
            List<String> errors = new ArrayList<>();

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
                List<String> validTypes = Arrays.asList("text", "password", "number", "boolean", "textarea", "select");
                if (!validTypes.contains(field.getType())) {
                    errors.add("invalid type: " + field.getType() + ". Valid types: " + validTypes);
                }
            }

            if (field.getRequired() == null) {
                field.setRequired(false);
            }

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

    private void sortConfigFields(List<ConfigField> configFields) {
        if (configFields != null && !configFields.isEmpty()) {
            configFields.sort(Comparator.comparingInt(f -> f.getOrder() != null ? f.getOrder() : Integer.MAX_VALUE));
        }
    }
}