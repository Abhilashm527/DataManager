package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.entity.Mappings;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.IdGenerator;
import com.dataflow.dataloaders.util.Identifier;
import com.fasterxml.jackson.core.JsonProcessingException;
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
public class MappingsDao extends GenericDaoImpl<Mappings, Identifier, String> {

    public static final String MAPPINGS_ID_PK = "mappings_pkey";
    public static final String UNIQUE_MAPPING_NAME = "unique_mapping_name_per_item";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    private IdGenerator idGenerator;

    @Override
    public Mappings create(@NotNull Mappings model, Identifier identifier) {
        return super.create(model, identifier);
    }

    @Override
    public Optional<Mappings> createV1(Mappings model, Identifier identifier) {
        try {
            // Generate custom ID if not present
            if (model.getId() == null || model.getId().isEmpty()) {
                model.setId(idGenerator.generateId());
            }
            String mappingId = insertMapping(model, identifier);
            return getV1(new Identifier(mappingId));
        } catch (DataIntegrityViolationException e) {
            handleDataIntegrityViolationException(e);
        } catch (Exception e) {
            handleGenericException(e);
        }
        throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION);
    }

    public String insertMapping(Mappings model, Identifier identifier) {
        jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    getSql("Mappings.create")
            );
            int idx = 1;
            ps.setObject(idx++, model.getId());
            ps.setObject(idx++, model.getMappingName());
            ps.setObject(idx++, toJson(model.getMappings()));
            ps.setObject(idx++, model.getIsActive());
            ps.setObject(idx++, model.getItemId());
            ps.setObject(idx++, model.getCreatedBy());
            ps.setObject(idx++, DateUtils.getUnixTimestampInUTC());

            return ps;
        });

        return model.getId();
    }

    @Override
    public Long insert(Mappings model, Identifier identifier) {
        return 0L;
    }

    @Override
    public Mappings upsert(Mappings model, Identifier identifier) {
        return super.upsert(model, identifier);
    }

    @Override
    public Mappings upsert(Mappings model, Identifier identifier, String whereClause) {
        return super.upsert(model, identifier, whereClause);
    }

    @Override
    public Mappings get(@NotNull Identifier identifier) {
        return super.get(identifier);
    }

    @Override
    public Optional<Mappings> getV1(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("Mappings.getById"),
                    mappingsRowMapper,
                    identifier.getWord()
            ));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public List<Mappings> listByItemId(String itemId) {
        try {
            return jdbcTemplate.query(
                    getSql("Mappings.getByItemId"),
                    mappingsRowMapper,
                    itemId
            );
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    @Override
    public Mappings get(Identifier identifier, String whereClause) {
        return super.get(identifier, whereClause);
    }

    @Override
    public Optional<Mappings> getV1(Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    public int updateMapping(Mappings mappings, Identifier identifier) {
        try {
            return jdbcTemplate.update(getSql("Mappings.updateById"),
                    mappings.getMappingName(),
                    toJson(mappings.getMappings()),
                    mappings.getIsActive(),
                    mappings.getUpdatedBy(),
                    DateUtils.getUnixTimestampInUTC(),
                    mappings.getId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public List<Mappings> list(Identifier identifier) {
        try {
            return jdbcTemplate.query(getSql("Mappings.getAll"), mappingsRowMapper);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    @Override
    public List<Mappings> list(Identifier identifier, String whereClause) {
        return List.of();
    }

    @Override
    public int count(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public Mappings update(Mappings transientObject, Identifier identifier) {
        return super.update(transientObject, identifier);
    }

    @Override
    public Optional<Mappings> updateV1(Mappings transientObject, Identifier identifier) {
        return Optional.empty();
    }

    @Override
    public Optional<Mappings> hotUpdate(Mappings transientObject, Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public int delete(Mappings mappings) {
        try {
            return jdbcTemplate.update(getSql("Mappings.deleteById"),
                    mappings.getUpdatedBy() != null ? mappings.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    DateUtils.getUnixTimestampInUTC(),
                    mappings.getId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public int deleteV1(Optional<Mappings> persistentObject) {
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

    private String toJson(List<com.dataflow.dataloaders.jobconfigs.InputField> inputField) {
        if (inputField == null) {
            return "{}";
        }
        try {
            return objectMapper.writeValueAsString(inputField);
        } catch (JsonProcessingException e) {
            log.error("Error converting InputField to JSON", e);
            return "{}";
        }
    }

    private List<com.dataflow.dataloaders.jobconfigs.InputField> fromJson(String json) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        try {
            return objectMapper.readValue(json, List.class);
        } catch (JsonProcessingException e) {
            log.error("Error parsing JSON to InputField", e);
            return null;
        }
    }

    private void handleDataIntegrityViolationException(DataIntegrityViolationException e) {
        String errorMessage = e.getRootCause() != null ? e.getRootCause().getMessage() : e.getMessage();
        log.error("DataIntegrityViolationException: {}", errorMessage);

        if (errorMessage != null) {
            // Check for primary key violation
            if (errorMessage.contains(MAPPINGS_ID_PK) || errorMessage.contains("mapping_id")) {
                throw new DataloadersException(ErrorFactory.DUPLICATION,
                        "A Mapping with this ID already exists");
            }
            // Check for unique name constraint violation
            if (errorMessage.contains(UNIQUE_MAPPING_NAME) || errorMessage.contains("mapping_name")) {
                throw new DataloadersException(ErrorFactory.DUPLICATION,
                        "A Mapping with this name already exists in this item");
            }
        }

        // Generic duplication error
        throw new DataloadersException(ErrorFactory.DUPLICATION,
                "A Mapping with this data already exists");
    }

    private void handleGenericException(Exception e) {
        log.error("Exception Occurred: {}", e.getMessage(), e);
        throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, e.getMessage());
    }

    // ==================== Row Mapper ====================

    public List<java.util.Map<String, Object>> searchByDescription(String query, Identifier identifier) {
        try {
            String sql = "SELECT mapping_id, mapping_name FROM mappings " +
                        "WHERE LOWER(mapping_name) LIKE LOWER(?) " +
                        "AND deleted_at IS NULL ORDER BY mapping_name";
            String searchPattern = "%" + query + "%";
            return jdbcTemplate.queryForList(sql, searchPattern);
        } catch (Exception e) {
            log.error("Error searching mappings: {}", e.getMessage());
            return List.of();
        }
    }

    RowMapper<Mappings> mappingsRowMapper = (rs, rowNum) -> {
        Mappings mappings = new Mappings();

        mappings.setId(rs.getObject("mapping_id") != null ? rs.getString("mapping_id") : null);
        mappings.setMappingName(rs.getObject("mapping_name") != null ? rs.getString("mapping_name") : null);

        // Parse mappings JSON to InputField object
        String mappingsJson = rs.getObject("mappings") != null ? rs.getString("mappings") : null;
        mappings.setMappings(fromJson(mappingsJson));

        mappings.setIsActive(rs.getObject("is_active") != null ? rs.getBoolean("is_active") : null);
        mappings.setItemId(rs.getObject("item_id") != null ? rs.getString("item_id") : null);

        // Audit fields
        mappings.setCreatedBy(rs.getObject("created_by") != null ? rs.getString("created_by") : null);
        mappings.setCreatedAt(rs.getObject("created_at") != null ? rs.getLong("created_at") : null);
        mappings.setUpdatedBy(rs.getObject("updated_by") != null ? rs.getString("updated_by") : null);
        mappings.setUpdatedAt(rs.getObject("updated_at") != null ? rs.getLong("updated_at") : null);
        mappings.setDeletedAt(rs.getObject("deleted_at") != null ? rs.getLong("deleted_at") : null);

        return mappings;
    };
}