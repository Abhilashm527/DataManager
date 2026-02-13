package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.entity.Datatable;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.IdGenerator;
import com.dataflow.dataloaders.util.Identifier;
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

    @Override
    public Optional<Datatable> createV1(Datatable model, Identifier identifier) {
        try {
            // Generate custom ID if not present
            if (model.getId() == null || model.getId().isEmpty()) {
                model.setId(idGenerator.generateId());
            }
            String resourceId = insertResource(model, identifier);
            return getV1(new Identifier(resourceId));
        } catch (DataIntegrityViolationException e) {
            handleDataIntegrityViolation(e, "Datatable");
        } catch (Exception e) {
            handleDatabaseException(e);
        }
        throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION);
    }

    public String insertResource(Datatable datatable, Identifier identifier) {
        try {
            jdbcTemplate.update(con -> {
                PreparedStatement ps = con.prepareStatement(
                        getSql("Datatable.create"));
                int idx = 1;
                ps.setObject(idx++, datatable.getId());
                ps.setObject(idx++, datatable.getDatatableId());
                ps.setObject(idx++, datatable.getApplicationId());
                ps.setObject(idx++, "admin");
                ps.setObject(idx++, DateUtils.getUnixTimestampInUTC());
                return ps;
            });
            return datatable.getId();
        } catch (Exception e) {
            handleDatabaseException(e);
            return null;
        }
    }

    @Override
    public Long insert(Datatable model, Identifier identifier) {
        return 0L;
    }

    @Override
    public Optional<Datatable> getV1(Identifier identifier) {
        return Optional.empty();
    }

    @Override
    public Optional<Datatable> getV1(Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public List<Datatable> list(Identifier identifier) {
        return List.of();
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

    RowMapper<Datatable> datatableMapper = (rs, rowNum) -> {
        Datatable datatable = new Datatable();
        datatable.setId(rs.getString("id"));
        datatable.setDatatableId(rs.getString("datatable_id"));
        datatable.setApplicationId(rs.getString("application_id"));
        datatable.setCreatedBy(rs.getString("created_by"));
        datatable.setCreatedAt(rs.getObject("created_at", Long.class));
        datatable.setUpdatedBy(rs.getString("updated_by"));
        datatable.setUpdatedAt(rs.getObject("updated_at", Long.class));
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
}
