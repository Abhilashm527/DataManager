package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.entity.Resource;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Slf4j
@Repository
public class ResourceDao extends GenericDaoImpl<Resource, Identifier, String> {

    public static final String RESOURCE_ID_PK = "resources_pkey";
    public static final String UNIQUE_RESOURCE_NAME = "unique_resource_name_per_item";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    private IdGenerator idGenerator;

    @Override
    public Resource create(@NotNull Resource model, Identifier identifier) {
        return super.create(model, identifier);
    }

    @Override
    public Optional<Resource> createV1(Resource model, Identifier identifier) {
        try {
            // Generate custom ID if not present
            if (model.getId() == null || model.getId().isEmpty()) {
                model.setId(idGenerator.generateId());
            }
            String resourceId = insertResource(model, identifier);
            return getV1(new Identifier(resourceId));
        } catch (DataIntegrityViolationException e) {
            handleDataIntegrityViolationException(e);
        } catch (Exception e) {
            handleGenericException(e);
        }
        throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION);
    }

    public String insertResource(Resource model, Identifier identifier) {
        jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    getSql("Resource.create")
            );
            int idx = 1;
            ps.setObject(idx++, model.getId());
            ps.setObject(idx++, model.getResourceType());
            ps.setObject(idx++, model.getResourceName());
            ps.setObject(idx++, model.getDescription());

            // Configuration as JSON string
            ps.setObject(idx++, toJson(model.getConfiguration()));

            // Status fields
            ps.setObject(idx++, model.getStatus());
            ps.setObject(idx++, model.getLastTestedAt());
            ps.setObject(idx++, model.getIsActive());

            // Display order
            ps.setObject(idx++, model.getDisplayOrder());

            // Audit fields
            ps.setObject(idx++, model.getCreatedBy());
            ps.setObject(idx++, DateUtils.getUnixTimestampInUTC());
            ps.setObject(idx++, model.getItemId());

            return ps;
        });

        return model.getId();
    }

    @Override
    public Long insert(Resource model, Identifier identifier) {
        return 0L;
    }

    @Override
    public Resource upsert(Resource model, Identifier identifier) {
        return super.upsert(model, identifier);
    }

    @Override
    public Resource upsert(Resource model, Identifier identifier, String whereClause) {
        return super.upsert(model, identifier, whereClause);
    }

    @Override
    public Resource get(@NotNull Identifier identifier) {
        return super.get(identifier);
    }

    @Override
    public Optional<Resource> getV1(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("Resource.getById"),
                    resourceRowMapper,
                    identifier.getWord()
            ));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public List<Resource> listByItemId(String word) {
        try {
            return jdbcTemplate.query(getSql("Resource.getByItemId"), resourceRowMapper, word);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    @Override
    public Resource get(Identifier identifier, String whereClause) {
        return super.get(identifier, whereClause);
    }

    @Override
    public Optional<Resource> getV1(Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    public int updateResource(Resource resource, Identifier identifier) {
        try {
            return jdbcTemplate.update(getSql("Resource.updateById"),
                    resource.getResourceName(),
                    resource.getDescription(),
                    toJson(resource.getConfiguration()),
                    resource.getStatus(),
                    resource.getIsActive(),
                    resource.getDisplayOrder(),
                    resource.getUpdatedBy(),
                    DateUtils.getUnixTimestampInUTC(),
                    resource.getId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    public int updateDisplayOrder(String resourceId, long displayOrder) {
        try {
            return jdbcTemplate.update(getSql("Resource.updateDisplayOrder"),
                    displayOrder,
                    "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    resourceId);
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    public int updateResourceStatus(String resourceId, String status, Long lastTestedAt) {
        try {
            return jdbcTemplate.update(getSql("Resource.updateStatus"),
                    status,
                    lastTestedAt,
                    "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    resourceId);
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public List<Resource> list(Identifier identifier) {
        try {
            return jdbcTemplate.query(getSql("Resource.getAll"), resourceRowMapper);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public List<Resource> listByResourceType(String resourceType) {
        try {
            return jdbcTemplate.query(
                    getSql("Resource.getByResourceType"),
                    resourceRowMapper,
                    resourceType
            );
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public List<Resource> listSortedByOrder(Identifier identifier) {
        try {
            return jdbcTemplate.query(
                    getSql("Resource.getAllSortedByOrder"),
                    resourceRowMapper
            );
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public long getMaxDisplayOrder() {
        try {
            Long maxOrder = jdbcTemplate.queryForObject(
                    getSql("Resource.getMaxDisplayOrder"),
                    Long.class
            );
            return maxOrder != null ? maxOrder : 0L;
        } catch (Exception e) {
            log.warn("Error getting max display order, returning 0", e);
            return 0L;
        }
    }

    @Override
    public List<Resource> list(Identifier identifier, String whereClause) {
        return List.of();
    }

    @Override
    public int count(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public Resource update(Resource transientObject, Identifier identifier) {
        return super.update(transientObject, identifier);
    }

    @Override
    public Optional<Resource> updateV1(Resource transientObject, Identifier identifier) {
        return Optional.empty();
    }

    @Override
    public Optional<Resource> hotUpdate(Resource transientObject, Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public int delete(Resource resource) {
        try {
            return jdbcTemplate.update(getSql("Resource.deleteById"),
                    resource.getUpdatedBy() != null ? resource.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    DateUtils.getUnixTimestampInUTC(),
                    resource.getId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public int deleteV1(Optional<Resource> persistentObject) {
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

    private String toJson(Map<String, Object> map) {
        if (map == null) {
            return "{}";
        }
        try {
            return objectMapper.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            log.error("Error converting map to JSON", e);
            return "{}";
        }
    }

    private Map<String, Object> fromJson(String json) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        try {
            return objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {});
        } catch (JsonProcessingException e) {
            log.error("Error parsing JSON to map", e);
            return null;
        }
    }

    private void handleDataIntegrityViolationException(DataIntegrityViolationException e) {
        String errorMessage = e.getRootCause() != null ? e.getRootCause().getMessage() : e.getMessage();
        log.error("DataIntegrityViolationException: {}", errorMessage);

        if (errorMessage != null) {
            // Check for primary key violation
            if (errorMessage.contains(RESOURCE_ID_PK) || errorMessage.contains("resource_id")) {
                throw new DataloadersException(ErrorFactory.DUPLICATION,
                        "A Resource with this ID already exists");
            }
            // Check for unique name constraint violation
            if (errorMessage.contains(UNIQUE_RESOURCE_NAME) || errorMessage.contains("resource_name")) {
                throw new DataloadersException(ErrorFactory.DUPLICATION,
                        "A Resource with this name already exists in this application");
            }
        }

        // Generic duplication error
        throw new DataloadersException(ErrorFactory.DUPLICATION,
                "A Resource with this data already exists");
    }

    private void handleGenericException(Exception e) {
        log.error("Exception Occurred: {}", e.getMessage(), e);
        throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, e.getMessage());
    }

    // ==================== Row Mapper ====================

    public List<java.util.Map<String, Object>> searchByName(String query, Identifier identifier) {
        try {
            String sql = "SELECT resource_id, resource_name, description, resource_type FROM resources " +
                        "WHERE (LOWER(resource_name) LIKE LOWER(?) OR LOWER(description) LIKE LOWER(?)) " +
                        "AND deleted_at IS NULL ORDER BY resource_name";
            String searchPattern = "%" + query + "%";
            return jdbcTemplate.queryForList(sql, searchPattern, searchPattern);
        } catch (Exception e) {
            log.error("Error searching resources: {}", e.getMessage());
            return List.of();
        }
    }

    RowMapper<Resource> resourceRowMapper = (rs, rowNum) -> {
        Resource resource = new Resource();

        resource.setId(rs.getObject("resource_id") != null ? rs.getString("resource_id") : null);
        resource.setResourceType(rs.getObject("resource_type") != null ? rs.getString("resource_type") : null);
        resource.setResourceName(rs.getObject("resource_name") != null ? rs.getString("resource_name") : null);
        resource.setDescription(rs.getObject("description") != null ? rs.getString("description") : null);

        // Parse configuration JSON
        String configJson = rs.getObject("configuration") != null ? rs.getString("configuration") : null;
        resource.setConfiguration(fromJson(configJson));

        // Status fields
        resource.setStatus(rs.getObject("status") != null ? rs.getString("status") : null);
        resource.setLastTestedAt(rs.getObject("last_tested_at") != null ? rs.getLong("last_tested_at") : null);
        resource.setIsActive(rs.getObject("is_active") != null ? rs.getBoolean("is_active") : null);

        // Display order
        resource.setDisplayOrder(rs.getObject("display_order") != null ? rs.getLong("display_order") : 0L);
        resource.setItemId(rs.getObject("item_id") != null ? rs.getString("item_id") : null);

        // Audit fields
        resource.setCreatedBy(rs.getObject("created_by") != null ? rs.getString("created_by") : null);
        resource.setCreatedAt(rs.getObject("created_at") != null ? rs.getLong("created_at") : null);
        resource.setUpdatedBy(rs.getObject("updated_by") != null ? rs.getString("updated_by") : null);
        resource.setUpdatedAt(rs.getObject("updated_at") != null ? rs.getLong("updated_at") : null);

        return resource;
    };
}