package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.entity.ActivityAction;
import com.dataflow.dataloaders.entity.ActivityDefinition;
import com.dataflow.dataloaders.entity.ConfigSchema;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.DFUtil;
import com.dataflow.dataloaders.util.IdGenerator;
import com.dataflow.dataloaders.util.Identifier;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Slf4j
@Repository
public class ActivityDefinitionDao extends GenericDaoImpl<ActivityDefinition, Identifier, String> {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    private DFUtil dfUtil;

    @Autowired
    private IdGenerator idGenerator;

    @Override
    public Optional<ActivityDefinition> createV1(ActivityDefinition model, Identifier identifier) {
        try {
            if (model.getId() == null || model.getId().isEmpty()) {
                model.setId(idGenerator.generateId());
            }
            String id = insertActivityDefinition(model, identifier);
            return getV1(Identifier.builder().word(id).build());
        } catch (Exception e) {
            handleDatabaseException(e);
            return Optional.empty();
        }
    }

    @Override
    public Long insert(ActivityDefinition model, Identifier identifier) {
        return 0L;
    }

    public String insertActivityDefinition(ActivityDefinition model, Identifier identifier) {
        KeyHolder holder = new GeneratedKeyHolder();
        jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(getSql("ActivityDefinition.create"), new String[] { "id" });
            ps.setString(1, model.getId());
            ps.setString(2, model.getActivityType());
            ps.setString(3, model.getCategory());
            ps.setString(4, model.getLabel());
            ps.setString(5, model.getDescription());
            ps.setString(6, model.getIconStr());
            ps.setObject(7, dfUtil.writeValueAsString(model.getSupportedConnectionTypes()), java.sql.Types.OTHER);
            ps.setObject(8, dfUtil.writeValueAsString(model.getConfigSchema()), java.sql.Types.OTHER);
            ps.setObject(9, dfUtil.writeValueAsString(model.getActivityActions()), java.sql.Types.OTHER);
            ps.setObject(10, model.getCreatedBy() != null ? model.getCreatedBy() : "admin");
            ps.setObject(11, DateUtils.getUnixTimestampInUTC());
            return ps;
        }, holder);
        return model.getId();
    }

    @Override
    public Optional<ActivityDefinition> getV1(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("ActivityDefinition.getById"), activityDefinitionRowMapper, identifier.getWord()));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    @Override
    public List<ActivityDefinition> list(Identifier identifier) {
        try {
            return jdbcTemplate.query(getSql("ActivityDefinition.list", identifier), activityDefinitionRowMapper);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public int update(ActivityDefinition model) {
        try {
            return jdbcTemplate.update(getSql("ActivityDefinition.updateById"),
                    model.getActivityType(),
                    model.getCategory(),
                    model.getLabel(),
                    model.getDescription(),
                    model.getIconStr(),
                    dfUtil.writeValueAsString(model.getSupportedConnectionTypes()),
                    dfUtil.writeValueAsString(model.getConfigSchema()),
                    dfUtil.writeValueAsString(model.getActivityActions()),
                    model.getUpdatedBy() != null ? model.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    model.getId());
        } catch (Exception e) {
            handleDatabaseException(e);
            return 0;
        }
    }

    @Override
    public int delete(ActivityDefinition model) {
        try {
            return jdbcTemplate.update(getSql("ActivityDefinition.deleteById"),
                    model.getUpdatedBy() != null ? model.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    DateUtils.getUnixTimestampInUTC(),
                    model.getId());
        } catch (Exception e) {
            handleDatabaseException(e);
            return 0;
        }
    }

    @Override
    public Optional<ActivityDefinition> getV1(Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public List<ActivityDefinition> list(Identifier identifier, String whereClause) {
        return List.of();
    }

    @Override
    public int count(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public Optional<ActivityDefinition> updateV1(ActivityDefinition transientObject, Identifier identifier) {
        return Optional.empty();
    }

    @Override
    public Optional<ActivityDefinition> hotUpdate(ActivityDefinition transientObject, Identifier identifier,
            String whereClause) {
        return Optional.empty();
    }

    @Override
    public int delete(Identifier identifier, String whereClause) {
        return 0;
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

    RowMapper<ActivityDefinition> activityDefinitionRowMapper = (rs, rowNum) -> {
        ActivityDefinition model = new ActivityDefinition();
        model.setId(rs.getString("id"));
        model.setActivityType(rs.getString("activity_type"));
        model.setCategory(rs.getString("category"));
        model.setLabel(rs.getString("label"));
        model.setDescription(rs.getString("description"));
        model.setIconStr(rs.getString("icon_str"));

        String connectionTypesJson = rs.getString("supported_connection_types");
        if (connectionTypesJson != null) {
            model.setSupportedConnectionTypes(dfUtil.readValue(new TypeReference<List<String>>() {
            }, connectionTypesJson));
        }

        String configSchemaJson = rs.getString("config_schema");
        if (configSchemaJson != null) {
            model.setConfigSchema(dfUtil.readValue(ConfigSchema.class, configSchemaJson));
        }

        String activityActionsJson = rs.getString("activity_actions");
        if (activityActionsJson != null) {
            model.setActivityActions(dfUtil.readValue(new TypeReference<List<ActivityAction>>() {
            }, activityActionsJson));
        }

        model.setCreatedAt(rs.getObject("created_at") != null ? rs.getLong("created_at") : null);
        model.setCreatedBy(rs.getString("created_by"));
        model.setUpdatedAt(rs.getObject("updated_at") != null ? rs.getLong("updated_at") : null);
        model.setUpdatedBy(rs.getString("updated_by"));
        model.setDeletedAt(rs.getObject("deleted_at") != null ? rs.getLong("deleted_at") : null);

        try {
            model.setTotal(rs.getObject("total") != null ? rs.getLong("total") : null);
        } catch (Exception e) {
            // Ignored if column doesn't exist
        }

        return model;
    };
}
