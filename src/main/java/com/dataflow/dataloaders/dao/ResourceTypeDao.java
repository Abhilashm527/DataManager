package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.entity.ConfigField;
import com.dataflow.dataloaders.entity.ResourceType;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.Identifier;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Slf4j
@Repository
public class ResourceTypeDao extends GenericDaoImpl<ResourceType, Identifier, String> {

    public static final String RESOURCE_TYPE_ID = "resource_type_id_pk";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Override
    public ResourceType create(@NotNull ResourceType model, Identifier identifier) {
        return super.create(model, identifier);
    }

    @Override
    public Optional<ResourceType> createV1(ResourceType model, Identifier identifier) {
        try {
            // Generate custom ID if not present: RT-{SerialNumber}
            if (model.getId() == null || model.getId().isEmpty()) {
                model.setId(generateResourceTypeId());
            }
            String resourceTypeId = insertResourceType(model, identifier);
            return getV1(new Identifier(resourceTypeId));
        } catch (DataIntegrityViolationException e) {
            handleDataIntegrityViolationException(e);
        } catch (Exception e) {
            handleGenericException(e);
        }
        throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION);
    }

    /**
     * Generate resource type ID in format: RT-{SerialNumber}
     * Example: RT-000001, RT-000002, etc.
     */
    private String generateResourceTypeId() {
        try {
            Long maxSerial = jdbcTemplate.queryForObject(
                    getSql("ResourceType.getMaxSerial"),
                    Long.class
            );
            long nextSerial = (maxSerial != null ? maxSerial : 0L) + 1;
            String resourceTypeId = String.format("RT-%06d", nextSerial);
            log.info("Generated resource type ID: {}", resourceTypeId);
            return resourceTypeId;
        } catch (Exception e) {
            log.error("Error generating resource type ID", e);
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION,
                    "Failed to generate resource type ID");
        }
    }

    public String insertResourceType(ResourceType model, Identifier identifier) {
        jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    getSql("ResourceType.create")
            );
            int idx = 1;
            ps.setObject(idx++, model.getId());
            ps.setObject(idx++, model.getTypeName());
            ps.setObject(idx++, model.getDescription());

            // ConfigFields as JSON string
            ps.setObject(idx++, toJson(model.getConfigFields()));

            // Display order
            ps.setObject(idx++, model.getDisplayOrder());

            // Status
            ps.setObject(idx++, model.getIsActive());

            // Audit fields
            ps.setObject(idx++, model.getCreatedBy());
            ps.setObject(idx++, DateUtils.getUnixTimestampInUTC());

            return ps;
        });

        return model.getId();
    }

    @Override
    public Long insert(ResourceType model, Identifier identifier) {
        return 0L;
    }

    @Override
    public ResourceType upsert(ResourceType model, Identifier identifier) {
        return super.upsert(model, identifier);
    }

    @Override
    public ResourceType upsert(ResourceType model, Identifier identifier, String whereClause) {
        return super.upsert(model, identifier, whereClause);
    }

    @Override
    public ResourceType get(@NotNull Identifier identifier) {
        return super.get(identifier);
    }

    @Override
    public Optional<ResourceType> getV1(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("ResourceType.getById"),
                    resourceTypeRowMapper,
                    identifier.getWord()
            ));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    @Override
    public ResourceType get(Identifier identifier, String whereClause) {
        return super.get(identifier, whereClause);
    }

    @Override
    public Optional<ResourceType> getV1(Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    public Optional<ResourceType> getByTypeName(String typeName) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("ResourceType.getByTypeName"),
                    resourceTypeRowMapper,
                    typeName
            ));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public int updateResourceType(ResourceType resourceType, Identifier identifier) {
        try {
            return jdbcTemplate.update(getSql("ResourceType.updateById"),
                    resourceType.getTypeName(),
                    resourceType.getDescription(),
                    toJson(resourceType.getConfigFields()),
                    resourceType.getDisplayOrder(),
                    resourceType.getIsActive(),
                    resourceType.getUpdatedBy(),
                    DateUtils.getUnixTimestampInUTC(),
                    resourceType.getId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    public int updateDisplayOrder(String resourceTypeId, long displayOrder) {
        try {
            return jdbcTemplate.update(getSql("ResourceType.updateDisplayOrder"),
                    displayOrder,
                    "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    resourceTypeId);
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    public int updateResourceTypeStatus(String resourceTypeId, Boolean isActive) {
        try {
            return jdbcTemplate.update(getSql("ResourceType.updateStatus"),
                    isActive,
                    "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    resourceTypeId);
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public List<ResourceType> list(Identifier identifier) {
        try {
            return jdbcTemplate.query(getSql("ResourceType.getAll"), resourceTypeRowMapper);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public List<ResourceType> listActive() {
        try {
            return jdbcTemplate.query(getSql("ResourceType.getAllActive"), resourceTypeRowMapper);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }



    public List<ResourceType> listSortedByOrder(Identifier identifier) {
        try {
            return jdbcTemplate.query(
                    getSql("ResourceType.getAllSortedByOrder"),
                    resourceTypeRowMapper
            );
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public long getMaxDisplayOrder() {
        try {
            Long maxOrder = jdbcTemplate.queryForObject(
                    getSql("ResourceType.getMaxDisplayOrder"),
                    Long.class
            );
            return maxOrder != null ? maxOrder : 0L;
        } catch (Exception e) {
            log.warn("Error getting max display order, returning 0", e);
            return 0L;
        }
    }

    @Override
    public List<ResourceType> list(Identifier identifier, String whereClause) {
        return List.of();
    }

    @Override
    public int count(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public ResourceType update(ResourceType transientObject, Identifier identifier) {
        return super.update(transientObject, identifier);
    }

    @Override
    public Optional<ResourceType> updateV1(ResourceType transientObject, Identifier identifier) {
        return Optional.empty();
    }

    @Override
    public Optional<ResourceType> hotUpdate(ResourceType transientObject, Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public int delete(ResourceType resourceType) {
        try {
            return jdbcTemplate.update(getSql("ResourceType.deleteById"),
                    resourceType.getUpdatedBy() != null ? resourceType.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    DateUtils.getUnixTimestampInUTC(),
                    resourceType.getId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public int deleteV1(Optional<ResourceType> persistentObject) {
        return super.deleteV1(persistentObject);
    }

    @Override
    public int delete(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public int findAndDelete(Identifier identifier) {
        return super.findAndDelete(identifier);
    }

    @Override
    public <E extends Number> String setInvalues(String query, String replaceString, Set<E> inValues, String... delimitter) {
        return super.setInvalues(query, replaceString, inValues, delimitter);
    }

    @Override
    public String setInvalues(String query, String replaceString, Set<String> inValues) {
        return super.setInvalues(query, replaceString, inValues);
    }

    // ==================== Helper Methods ====================

    private String toJson(List<ConfigField> list) {
        if (list == null) {
            return "[]";
        }
        try {
            return objectMapper.writeValueAsString(list);
        } catch (JsonProcessingException e) {
            log.error("Error converting list to JSON", e);
            return "[]";
        }
    }

    private List<ConfigField> fromJson(String json) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        try {
            return objectMapper.readValue(json, new TypeReference<List<ConfigField>>() {});
        } catch (JsonProcessingException e) {
            log.error("Error parsing JSON to list", e);
            return null;
        }
    }

    private void handleDataIntegrityViolationException(DataIntegrityViolationException e) {
        String errorMessage = e.getRootCause() != null ? e.getRootCause().getMessage() : e.getMessage();
        if (errorMessage != null) {
            if (errorMessage.contains(RESOURCE_TYPE_ID)) {
                throw new DataloadersException(ErrorFactory.DUPLICATION,
                        "A Resource Type with this ID already exists");
            }
            if (errorMessage.contains("type_name")) {
                throw new DataloadersException(ErrorFactory.DUPLICATION,
                        "A Resource Type with this name already exists");
            }
        }
        throw new DataloadersException(ErrorFactory.DUPLICATION,
                "A Resource Type with this data already exists");
    }

    private void handleGenericException(Exception e) {
        logger.error("Exception Occurred: {}", e.getMessage());
        throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, e.getMessage());
    }

    // ==================== Row Mapper ====================

    RowMapper<ResourceType> resourceTypeRowMapper = (rs, rowNum) -> {
        ResourceType resourceType = new ResourceType();

        resourceType.setId(rs.getObject("resource_type_id") != null ? rs.getString("resource_type_id") : null);
        resourceType.setTypeName(rs.getObject("type_name") != null ? rs.getString("type_name") : null);
        resourceType.setDescription(rs.getObject("description") != null ? rs.getString("description") : null);

        // Parse configFields JSON
        String configFieldsJson = rs.getObject("config_fields") != null ? rs.getString("config_fields") : null;
        resourceType.setConfigFields(fromJson(configFieldsJson));

        // Display order
        resourceType.setDisplayOrder(rs.getObject("display_order") != null ? rs.getLong("display_order") : 0L);

        // Status
        resourceType.setIsActive(rs.getObject("is_active") != null ? rs.getBoolean("is_active") : null);

        // Audit fields
        resourceType.setCreatedBy(rs.getObject("created_by") != null ? rs.getString("created_by") : null);
        resourceType.setCreatedAt(rs.getObject("created_at") != null ? rs.getLong("created_at") : null);
        resourceType.setUpdatedBy(rs.getObject("updated_by") != null ? rs.getString("updated_by") : null);
        resourceType.setUpdatedAt(rs.getObject("updated_at") != null ? rs.getLong("updated_at") : null);

        return resourceType;
    };
}