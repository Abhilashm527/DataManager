package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dao.MappingsDao;
import com.dataflow.dataloaders.entity.Mappings;
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
public class MappingsService {

    protected static final String logPrefix = "{} : {}";

    @Autowired
    private MappingsDao mappingsDao;

    @Transactional
    public Mappings create(Mappings mappings, Identifier identifier) {
        log.info(logPrefix, this.getClass().getSimpleName(), "create - identifier: {}", identifier);

        // Validate mapping name
        if (mappings.getMappingName() == null || mappings.getMappingName().trim().isEmpty()) {
            throw new DataloadersException(ErrorFactory.VALIDATION_ERROR, "Mapping name is required");
        }
        // Set default values
        mappings.setIsActive(false);
        mappings.setCreatedBy("admin");
        mappings.setCreatedAt(DateUtils.getUnixTimestampInUTC());

        Mappings created = mappingsDao.create(mappings, identifier);
        log.info("Created mapping: {} with name: {}", created.getId(), created.getMappingName());
        return created;
    }

    public Mappings getMapping(Identifier identifier) {
        log.info(logPrefix, this.getClass().getSimpleName(), "getMapping - identifier: {}", identifier);
        Mappings mappings = mappingsDao.getV1(identifier)
                .orElseThrow(() -> {
                    log.warn("Mapping not found for identifier: {}", identifier);
                    return new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND);
                });
        return mappings;
    }

    public List<Mappings> getAllMappings(Identifier identifier) {
        log.info(logPrefix, this.getClass().getSimpleName(), "getAllMappings - identifier: {}", identifier);
        List<Mappings> mappingsList = mappingsDao.list(identifier);
        if (mappingsList.isEmpty()) {
            log.warn("No mappings found for identifier: {}", identifier);
            return Collections.emptyList();
        }
        log.info("Found {} mappings", mappingsList.size());
        return mappingsList;
    }

    public List<Mappings> getMappingsByItemId(Identifier identifier) {
        log.info(logPrefix, this.getClass().getSimpleName(),
                "getMappingsByItemId - itemId: {}", identifier.getWord());
        List<Mappings> mappingsList = mappingsDao.listByItemId(identifier.getWord());
        if (mappingsList.isEmpty()) {
            log.warn("No mappings found for itemId: {}", identifier.getWord());
            return Collections.emptyList();
        }
        log.info("Found {} mappings for itemId: {}", mappingsList.size(), identifier.getWord());
        return mappingsList;
    }

    @Transactional
    public Object deleteMapping(Identifier identifier) {
        log.info("deleteMapping - identifier: {}", identifier);
        Mappings mappings = mappingsDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));

        mappingsDao.delete(mappings);
        log.info("Deleted mapping: {}", identifier);
        return true;
    }

    @Transactional
    public Mappings updateMapping(Mappings mappings, Identifier identifier) {
        log.info("updateMapping - identifier: {}, mappings: {}", identifier, mappings);
        Optional<Mappings> existingMapping = mappingsDao.getV1(identifier);
        if (existingMapping.isEmpty()) {
            log.warn("Mapping not found for update - identifier: {}", identifier);
            throw new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND);
        }

        Mappings existing = existingMapping.get();

        // Update basic fields
        if (mappings.getMappingName() != null) {
            existing.setMappingName(mappings.getMappingName());
        }

        // Update mappings (InputField object)
        if (mappings.getMappings() != null) {
            existing.setMappings(mappings.getMappings());
        }

        existing.setUpdatedBy("admin");
        existing.setUpdatedAt(DateUtils.getUnixTimestampInUTC());

        boolean updated = mappingsDao.updateMapping(existing, identifier) > 0;
        if (!updated) {
            log.error("Failed to update mapping: {}", existing.getId());
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, "Not updated");
        }
        log.info("Updated mapping: {}", existing.getId());
        return mappingsDao.getV1(identifier).orElse(existing);
    }

    @Transactional
    public Mappings updateMappingStatus(Identifier identifier, Boolean isActive) {
        log.info("updateMappingStatus - identifier: {}, isActive: {}", identifier, isActive);
        Mappings mappings = mappingsDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));

        mappings.setIsActive(isActive);
        mappings.setUpdatedBy("admin");
        mappings.setUpdatedAt(DateUtils.getUnixTimestampInUTC());

        mappingsDao.updateMapping(mappings, identifier);
        log.info("Updated mapping status: {} to {}", mappings.getId(), isActive);
        return mappings;
    }

    public Map<String, Object> validateMapping(Mappings mappings, Identifier identifier) {
        log.info("validateMapping - mappingName: {}", mappings.getMappingName());
        Map<String, Object> result = new HashMap<>();

        try {
            // Validate mapping name
            if (mappings.getMappingName() == null || mappings.getMappingName().trim().isEmpty()) {
                result.put("valid", false);
                result.put("message", "Mapping name is required");
                return result;
            }

            result.put("valid", true);
            result.put("message", "Mapping configuration is valid");
        } catch (Exception e) {
            log.error("Mapping validation failed: {}", e.getMessage());
            result.put("valid", false);
            result.put("message", "Mapping validation failed: " + e.getMessage());
        }

        return result;
    }
}