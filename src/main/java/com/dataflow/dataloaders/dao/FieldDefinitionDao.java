package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.entity.ConfigField;
import com.dataflow.dataloaders.entity.FieldDefinition;
import com.dataflow.dataloaders.enums.Provider;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.IdGenerator;
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
public class FieldDefinitionDao extends GenericDaoImpl<FieldDefinition, Identifier, String> {

    public static final String FIELD_DEFINITION_ID = "field_definition_id_pk";

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    protected JdbcTemplate jdbcTemplate;
    @Autowired
    private IdGenerator idGenerator;

    @Override
    public FieldDefinition create(@NotNull FieldDefinition model, Identifier identifier) {
        return super.create(model, identifier);
    }

    @Override
    public Optional<FieldDefinition> createV1(FieldDefinition model, Identifier identifier) {
        try {
            if (model.getId() == null || model.getId().isEmpty()) {
                model.setId(idGenerator.generateId());
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

    public String insertResourceType(FieldDefinition model, Identifier identifier) {
        jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    getSql("FieldDefinition.create"));
            int idx = 1;
            ps.setObject(idx++, model.getId());
            ps.setObject(idx++, model.getTypeName());
            ps.setObject(idx++, model.getProvider() != null ? model.getProvider().name() : null);
            ps.setObject(idx++, model.getDescription());
            ps.setObject(idx++, toJson(model.getConfigFields()));
            ps.setObject(idx++, model.getDisplayOrder());
            ps.setObject(idx++, model.getIsActive());
            ps.setObject(idx++, model.getCreatedBy());
            ps.setObject(idx++, DateUtils.getUnixTimestampInUTC());
            return ps;
        });
        return model.getId();
    }

    @Override
    public Long insert(FieldDefinition model, Identifier identifier) {
        return 0L;
    }

    @Override
    public FieldDefinition upsert(FieldDefinition model, Identifier identifier) {
        return super.upsert(model, identifier);
    }

    @Override
    public FieldDefinition upsert(FieldDefinition model, Identifier identifier, String whereClause) {
        return super.upsert(model, identifier, whereClause);
    }

    @Override
    public FieldDefinition get(@NotNull Identifier identifier) {
        return super.get(identifier);
    }

    @Override
    public Optional<FieldDefinition> getV1(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("FieldDefinition.getById"),
                    fieldDefinitionRowMapper,
                    identifier.getWord()));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    @Override
    public FieldDefinition get(Identifier identifier, String whereClause) {
        return super.get(identifier, whereClause);
    }

    @Override
    public Optional<FieldDefinition> getV1(Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    public Optional<FieldDefinition> getByTypeName(String typeName) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("FieldDefinition.getByTypeName"),
                    fieldDefinitionRowMapper,
                    typeName));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public Optional<FieldDefinition> getByTypeNameAndProvider(String typeName, Provider provider) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("FieldDefinition.getByTypeNameAndProvider"),
                    fieldDefinitionRowMapper,
                    typeName, provider != null ? provider.name() : null));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public int updateResourceType(FieldDefinition resourceType, Identifier identifier) {
        try {
            return jdbcTemplate.update(getSql("FieldDefinition.updateById"),
                    resourceType.getTypeName(),
                    resourceType.getProvider() != null ? resourceType.getProvider().name() : null,
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
            return jdbcTemplate.update(getSql("FieldDefinition.updateDisplayOrder"),
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
            return jdbcTemplate.update(getSql("FieldDefinition.updateStatus"),
                    isActive,
                    "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    resourceTypeId);
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public List<FieldDefinition> list(Identifier identifier) {
        try {
            return jdbcTemplate.query(getSql("FieldDefinition.getAll"), fieldDefinitionRowMapper);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public List<FieldDefinition> listActive() {
        try {
            return jdbcTemplate.query(getSql("FieldDefinition.getAllActive"), fieldDefinitionRowMapper);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public List<FieldDefinition> listSortedByOrder(Identifier identifier) {
        try {
            return jdbcTemplate.query(
                    getSql("FieldDefinition.getAllSortedByOrder"),
                    fieldDefinitionRowMapper);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public List<FieldDefinition> getByProvider(Provider provider) {
        try {
            return jdbcTemplate.query(getSql("FieldDefinition.getByProvider"), fieldDefinitionRowMapper,
                    provider.name());
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public long getMaxDisplayOrder() {
        try {
            Long maxOrder = jdbcTemplate.queryForObject(
                    getSql("FieldDefinition.getMaxDisplayOrder"),
                    Long.class);
            return maxOrder != null ? maxOrder : 0L;
        } catch (Exception e) {
            log.warn("Error getting max display order, returning 0", e);
            return 0L;
        }
    }

    @Override
    public List<FieldDefinition> list(Identifier identifier, String whereClause) {
        return List.of();
    }

    @Override
    public int count(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public FieldDefinition update(FieldDefinition transientObject, Identifier identifier) {
        return super.update(transientObject, identifier);
    }

    @Override
    public Optional<FieldDefinition> updateV1(FieldDefinition transientObject, Identifier identifier) {
        return Optional.empty();
    }

    @Override
    public Optional<FieldDefinition> hotUpdate(FieldDefinition transientObject, Identifier identifier,
            String whereClause) {
        return Optional.empty();
    }

    @Override
    public int delete(FieldDefinition resourceType) {
        try {
            return jdbcTemplate.update(getSql("FieldDefinition.deleteById"),
                    resourceType.getUpdatedBy() != null ? resourceType.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    DateUtils.getUnixTimestampInUTC(),
                    resourceType.getId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public int deleteV1(Optional<FieldDefinition> persistentObject) {
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
    public <E extends Number> String setInvalues(String query, String replaceString, Set<E> inValues,
            String... delimitter) {
        return super.setInvalues(query, replaceString, inValues, delimitter);
    }

    @Override
    public String setInvalues(String query, String replaceString, Set<String> inValues) {
        return super.setInvalues(query, replaceString, inValues);
    }

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
            return objectMapper.readValue(json, new TypeReference<List<ConfigField>>() {
            });
        } catch (JsonProcessingException e) {
            log.error("Error parsing JSON to list", e);
            return null;
        }
    }

    private void handleDataIntegrityViolationException(DataIntegrityViolationException e) {
        String errorMessage = e.getRootCause() != null ? e.getRootCause().getMessage() : e.getMessage();
        if (errorMessage != null) {
            if (errorMessage.contains(FIELD_DEFINITION_ID)) {
                throw new DataloadersException(ErrorFactory.DUPLICATION,
                        "A Field Definition with this ID already exists");
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

    RowMapper<FieldDefinition> fieldDefinitionRowMapper = (rs, rowNum) -> {
        FieldDefinition fieldDefinition = new FieldDefinition();

        fieldDefinition.setId(rs.getObject("field_definition_id") != null ? rs.getString("field_definition_id") : null);
        fieldDefinition.setTypeName(rs.getObject("type_name") != null ? rs.getString("type_name") : null);

        // Handle provider enum
        String providerStr = rs.getObject("provider") != null ? rs.getString("provider") : null;
        if (providerStr != null) {
            try {
                fieldDefinition.setProvider(Provider.valueOf(providerStr));
            } catch (IllegalArgumentException e) {
                fieldDefinition.setProvider(null);
            }
        }

        fieldDefinition.setDescription(rs.getObject("description") != null ? rs.getString("description") : null);

        // Parse configFields JSON
        String configFieldsJson = rs.getObject("config_fields") != null ? rs.getString("config_fields") : null;
        fieldDefinition.setConfigFields(fromJson(configFieldsJson));

        // Display order
        fieldDefinition.setDisplayOrder(rs.getObject("display_order") != null ? rs.getLong("display_order") : 0L);

        // Status
        fieldDefinition.setIsActive(rs.getObject("is_active") != null ? rs.getBoolean("is_active") : null);

        // Audit fields
        fieldDefinition.setCreatedBy(rs.getObject("created_by") != null ? rs.getString("created_by") : null);
        fieldDefinition.setCreatedAt(rs.getObject("created_at") != null ? rs.getLong("created_at") : null);
        fieldDefinition.setUpdatedBy(rs.getObject("updated_by") != null ? rs.getString("updated_by") : null);
        fieldDefinition.setUpdatedAt(rs.getObject("updated_at") != null ? rs.getLong("updated_at") : null);

        return fieldDefinition;
    };
}