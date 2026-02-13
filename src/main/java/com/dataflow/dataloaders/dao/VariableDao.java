package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.entity.Variable;
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
public class VariableDao extends GenericDaoImpl<Variable, Identifier, String> {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    private IdGenerator idGenerator;

    @Override
    public Optional<Variable> createV1(Variable model, Identifier identifier) {
        try {
            if (model.getId() == null || model.getId().isEmpty()) {
                model.setId(idGenerator.generateId());
            }
            jdbcTemplate.update(getSql("Variable.create"),
                    model.getId(),
                    model.getGroupId(),
                    model.getVariableKey(),
                    model.getVariableValue(),
                    model.getIsSecret(),
                    model.getDescription(),
                    DateUtils.getUnixTimestampInUTC(),
                    model.getCreatedBy() != null ? model.getCreatedBy() : "admin");
            return getV1(Identifier.builder().word(model.getId()).build());
        } catch (org.springframework.dao.DataIntegrityViolationException e) {
            handleDataIntegrityViolation(e, "Variable");
        } catch (Exception e) {
            handleDatabaseException(e);
        }
        return Optional.empty();
    }

    @Override
    public Optional<Variable> getV1(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("Variable.getById"), variableRowMapper, identifier.getWord()));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        } catch (Exception e) {
            handleDatabaseException(e);
            return Optional.empty();
        }
    }

    public List<Variable> listByGroup(String groupId) {
        try {
            return jdbcTemplate.query(getSql("Variable.getByGroup"), variableRowMapper, groupId);
        } catch (Exception e) {
            handleDatabaseException(e);
            return List.of();
        }
    }

    @Override
    public List<Variable> list(Identifier identifier) {
        return List.of();
    }

    @Override
    public List<Variable> list(Identifier identifier, String whereClause) {
        return List.of();
    }

    @Override
    public int count(Identifier identifier, String whereClause) {
        return 0;
    }

    public int updateVariable(Variable variable) {
        try {
            return jdbcTemplate.update(getSql("Variable.updateById"),
                    variable.getVariableValue(),
                    variable.getDescription(),
                    variable.getUpdatedBy() != null ? variable.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    variable.getId());
        } catch (Exception e) {
            handleDatabaseException(e);
            return 0;
        }
    }

    @Override
    public int delete(Variable variable) {
        try {
            return jdbcTemplate.update(getSql("Variable.deleteById"),
                    variable.getUpdatedBy() != null ? variable.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    DateUtils.getUnixTimestampInUTC(),
                    variable.getId());
        } catch (Exception e) {
            handleDatabaseException(e);
            return 0;
        }
    }

    @Override
    public Optional<Variable> getV1(Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public Optional<Variable> updateV1(Variable transientObject, Identifier identifier) {
        return Optional.empty();
    }

    @Override
    public Optional<Variable> hotUpdate(Variable transientObject, Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public int delete(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public Long insert(Variable model, Identifier identifier) {
        return 0L;
    }

    RowMapper<Variable> variableRowMapper = (rs, rowNum) -> {
        Variable variable = Variable.builder()
                .id(rs.getString("id"))
                .groupId(rs.getString("group_id"))
                .variableKey(rs.getString("variable_key"))
                .variableValue(rs.getString("variable_value"))
                .isSecret(rs.getBoolean("is_secret"))
                .description(rs.getString("description"))
                .build();
        variable.setCreatedAt(rs.getObject("created_at", Long.class));
        variable.setCreatedBy(rs.getString("created_by"));
        variable.setUpdatedAt(rs.getObject("updated_at", Long.class));
        variable.setUpdatedBy(rs.getString("updated_by"));
        variable.setDeletedAt(rs.getObject("deleted_at", Long.class));
        return variable;
    };
}
