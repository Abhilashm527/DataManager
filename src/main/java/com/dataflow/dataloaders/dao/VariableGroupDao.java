package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.entity.VariableGroup;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.IdGenerator;
import com.dataflow.dataloaders.util.Identifier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public class VariableGroupDao extends GenericDaoImpl<VariableGroup, Identifier, String> {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    private IdGenerator idGenerator;

    @Override
    public Optional<VariableGroup> createV1(VariableGroup model, Identifier identifier) {
        try {
            if (model.getId() == null || model.getId().isEmpty()) {
                model.setId(idGenerator.generateId());
            }
            jdbcTemplate.update(getSql("VariableGroup.create"),
                    model.getId(),
                    model.getName(),
                    model.getApplicationId(),
                    model.getEnvironment(),
                    model.getDescription(),
                    model.getTags(),
                    model.getGroupColor(),
                    DateUtils.getUnixTimestampInUTC(),
                    model.getCreatedBy() != null ? model.getCreatedBy() : "admin");
            return getV1(Identifier.builder().word(model.getId()).build());
        } catch (org.springframework.dao.DataIntegrityViolationException e) {
            handleDataIntegrityViolation(e, "VariableGroup");
        } catch (Exception e) {
            handleDatabaseException(e);
        }
        return Optional.empty();
    }

    @Override
    public Optional<VariableGroup> getV1(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("VariableGroup.getById"), variableGroupRowMapper, identifier.getWord()));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        } catch (Exception e) {
            handleDatabaseException(e);
            return Optional.empty();
        }
    }

    public List<VariableGroup> listByContext(String applicationId, String environment) {
        try {
            return jdbcTemplate.query(getSql("VariableGroup.getAllByContext"), variableGroupRowMapper, applicationId,
                    environment);
        } catch (Exception e) {
            handleDatabaseException(e);
            return List.of();
        }
    }

    @Override
    public List<VariableGroup> list(Identifier identifier) {
        return List.of();
    }

    @Override
    public List<VariableGroup> list(Identifier identifier, String whereClause) {
        return List.of();
    }

    @Override
    public int count(Identifier identifier, String whereClause) {
        return 0;
    }

    public int updateGroup(VariableGroup group) {
        try {
            return jdbcTemplate.update(getSql("VariableGroup.updateById"),
                    group.getName(),
                    group.getDescription(),
                    group.getTags(),
                    group.getGroupColor(),
                    group.getUpdatedBy() != null ? group.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    group.getId());
        } catch (Exception e) {
            handleDatabaseException(e);
            return 0;
        }
    }

    @Override
    public int delete(VariableGroup group) {
        try {
            return jdbcTemplate.update(getSql("VariableGroup.deleteById"),
                    group.getUpdatedBy() != null ? group.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    DateUtils.getUnixTimestampInUTC(),
                    group.getId());
        } catch (Exception e) {
            handleDatabaseException(e);
            return 0;
        }
    }

    @Override
    public Optional<VariableGroup> getV1(Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public Optional<VariableGroup> updateV1(VariableGroup transientObject, Identifier identifier) {
        return Optional.empty();
    }

    @Override
    public Optional<VariableGroup> hotUpdate(VariableGroup transientObject, Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public int delete(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public Long insert(VariableGroup model, Identifier identifier) {
        return 0L;
    }

    RowMapper<VariableGroup> variableGroupRowMapper = (rs, rowNum) -> {
        VariableGroup group = VariableGroup.builder()
                .id(rs.getString("id"))
                .name(rs.getString("name"))
                .applicationId(rs.getString("application_id"))
                .environment(rs.getString("environment"))
                .description(rs.getString("description"))
                .tags(rs.getString("tags"))
                .groupColor(rs.getString("group_color"))
                .build();
        group.setCreatedAt(rs.getObject("created_at", Long.class));
        group.setCreatedBy(rs.getString("created_by"));
        group.setUpdatedAt(rs.getObject("updated_at", Long.class));
        group.setUpdatedBy(rs.getString("updated_by"));
        group.setDeletedAt(rs.getObject("deleted_at", Long.class));
        return group;
    };
}
