package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.entity.Datatable;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.DFUtil;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.IdGenerator;
import com.dataflow.dataloaders.util.Identifier;
import com.fasterxml.jackson.core.type.TypeReference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.Optional;

@Repository
public class DatatableDao extends GenericDaoImpl<Datatable, Identifier, String> {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    private IdGenerator idGenerator;

    @Autowired
    private DFUtil dfUtil;

    @Override
    public Optional<Datatable> createV1(Datatable model, Identifier identifier) {
        try {
            if (model.getId() == null || model.getId().isEmpty()) {
                model.setId(idGenerator.generateId());
            }
            insertDatatable(model, identifier);
            return getV1(new Identifier(model.getId()));
        } catch (DataIntegrityViolationException e) {
            handleDataIntegrityViolation(e, "Datatable");
        } catch (Exception e) {
            handleDatabaseException(e);
        }
        throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION);
    }

    public void insertDatatable(Datatable datatable, Identifier identifier) {
        try {
            jdbcTemplate.update(con -> {
                PreparedStatement ps = con.prepareStatement(getSql("Datatable.create"));
                int idx = 1;
                ps.setObject(idx++, datatable.getId());
                ps.setObject(idx++, datatable.getApplicationId());
                ps.setObject(idx++, datatable.getTableName());
                ps.setObject(idx++, datatable.getDescription());
                ps.setObject(idx++, datatable.getStatus() != null ? datatable.getStatus() : "ACTIVE");
                ps.setObject(idx++, dfUtil.writeValueAsString(datatable.getColumns()));
                ps.setObject(idx++, dfUtil.writeValueAsString(datatable.getMetadata()));
                ps.setObject(idx++, "admin");
                ps.setObject(idx++, DateUtils.getUnixTimestampInUTC());
                return ps;
            });
        } catch (Exception e) {
            handleDatabaseException(e);
        }
    }

    @Override
    public Optional<Datatable> getV1(Identifier identifier) {
        try {
            return Optional.ofNullable(
                    jdbcTemplate.queryForObject(getSql("Datatable.getById"), datatableMapper, identifier.getWord()));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        } catch (Exception e) {
            handleDatabaseException(e);
            return Optional.empty();
        }
    }

    @Override
    public List<Datatable> list(Identifier identifier) {
        try {
            return jdbcTemplate.query(getSql("Datatable.listPaged", identifier), datatableMapper);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        } catch (Exception e) {
            handleDatabaseException(e);
            return List.of();
        }
    }

    public int updateDatatable(Datatable datatable, Identifier identifier) {
        try {
            return jdbcTemplate.update(getSql("Datatable.updateById"),
                    datatable.getTableName(),
                    datatable.getDescription(),
                    datatable.getStatus(),
                    dfUtil.writeValueAsString(datatable.getColumns()),
                    dfUtil.writeValueAsString(datatable.getMetadata()),
                    "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    datatable.getId());
        } catch (Exception e) {
            handleDatabaseException(e);
            return 0;
        }
    }

    public List<Datatable> getByApplicationId(Identifier identifier) {
        try {
            return jdbcTemplate.query(getSql("Datatable.getByApplicationId"), datatableMapper, identifier.getWord());
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        } catch (Exception e) {
            handleDatabaseException(e);
            return List.of();
        }
    }

    public List<Datatable> searchInApplication(String applicationId, String search) {
        try {
            return jdbcTemplate.query(getSql("Datatable.searchInApplication"), datatableMapper, applicationId,
                    "%" + search + "%", "%" + search + "%");
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        } catch (Exception e) {
            handleDatabaseException(e);
            return List.of();
        }
    }

    public long countByApplicationId(String applicationId) {
        try {
            return jdbcTemplate.queryForObject(getSql("Datatable.countByApplicationId"), Long.class, applicationId);
        } catch (Exception e) {
            return 0L;
        }
    }

    RowMapper<Datatable> datatableMapper = (rs, rowNum) -> {
        Datatable datatable = new Datatable();
        datatable.setId(rs.getString("id"));
        datatable.setApplicationId(rs.getString("application_id"));
        datatable.setTableName(rs.getString("table_name"));
        datatable.setDescription(rs.getString("description"));
        datatable.setStatus(rs.getString("status"));

        String columnsJson = rs.getString("columns");
        if (columnsJson != null) {
            datatable.setColumns(dfUtil.readValue(new TypeReference<List<Datatable.ColumnDefinition>>() {
            }, columnsJson));
        }

        String metadataJson = rs.getString("metadata");
        if (metadataJson != null) {
            datatable.setMetadata(dfUtil.readValueToMap(metadataJson));
        }

        datatable.setCreatedBy(rs.getString("created_by"));
        datatable.setCreatedAt(rs.getObject("created_at", Long.class));
        datatable.setUpdatedBy(rs.getString("updated_by"));
        datatable.setUpdatedAt(rs.getObject("updated_at", Long.class));
        datatable.setDeletedAt(rs.getObject("deleted_at", Long.class));
        try {
            datatable.setTotal(rs.getObject("total") != null ? rs.getLong("total") : null);
        } catch (Exception ignored) {
        }
        return datatable;
    };

    public int deleteByDatatableId(Identifier identifier) {
        try {
            return jdbcTemplate.update(getSql("Datatable.DeleteByDatatableId"),
                    "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    DateUtils.getUnixTimestampInUTC(),
                    identifier.getWord());
        } catch (Exception e) {
            handleDatabaseException(e);
            return 0;
        }
    }

    @Override
    public Long insert(Datatable model, Identifier identifier) {
        return 0L;
    }

    @Override
    public Optional<Datatable> getV1(Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public List<Datatable> list(Identifier identifier, String whereClause) {
        return List.of();
    }

    @Override
    public int count(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public Optional<Datatable> updateV1(Datatable transientObject, Identifier identifier) {
        return Optional.empty();
    }

    @Override
    public Optional<Datatable> hotUpdate(Datatable transientObject, Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public int delete(Datatable persistentObject) {
        return 0;
    }

    @Override
    public int delete(Identifier identifier, String whereClause) {
        return 0;
    }
}
