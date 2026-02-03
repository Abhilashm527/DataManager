package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.controller.DatatableController;
import com.dataflow.dataloaders.entity.Datatable;
import com.dataflow.dataloaders.entity.Item;
import com.dataflow.dataloaders.entity.ItemType;
import com.dataflow.dataloaders.entity.Resource;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.IdGenerator;
import com.dataflow.dataloaders.util.Identifier;
import jakarta.validation.constraints.NotNull;
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
public class DatatableDao  extends GenericDaoImpl<Datatable, Identifier, String>{

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    private IdGenerator idGenerator;

    @Override
    public Datatable create(@NotNull Datatable model, Identifier identifier) {
        return super.create(model, identifier);
    }

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
            handleDataIntegrityViolationException(e);
        } catch (Exception e) {
            handleGenericException(e);
        }
        throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION);
    }

    public String insertResource(Datatable datatable, Identifier identifier) {
        jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    getSql("Datatable.create")
            );
            int idx = 1;
            ps.setObject(idx++, datatable.getId());
            ps.setObject(idx++, datatable.getDatatableId());
            ps.setObject(idx++, datatable.getApplicationId());
            ps.setObject(idx++, "admin");
            ps.setObject(idx++, DateUtils.getUnixTimestampInUTC());
            return ps;
        });

        return datatable.getId();
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

    private void handleDataIntegrityViolationException(DataIntegrityViolationException e) {
        String errorMessage = e.getRootCause() != null ? e.getRootCause().getMessage() : e.getMessage();
        throw new DataloadersException(ErrorFactory.DUPLICATION, "An Item with this data already exists");
    }

    private void handleGenericException(Exception e) {
        logger.error("Exception Occurred: {}", e.getMessage());
        throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, e.getMessage());
    }

    public List<Datatable> getByApplicationId(Identifier identifier) {
        try {
            return jdbcTemplate.query(getSql("Datatable.getByApplicationId"), datatableMapper, identifier.getWord());
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }
    RowMapper<Datatable> datatableMapper = (rs, rowNum) -> {
        Datatable datatable = new Datatable();
        datatable.setId(rs.getString("id"));
        datatable.setDatatableId(rs.getString("datatable_id"));
        datatable.setApplicationId(rs.getString("application_id"));
        datatable.setCreatedBy(rs.getString("created_by"));
        datatable.setCreatedAt(rs.getObject("created_at") != null ? rs.getLong("created_at") : null);
        datatable.setUpdatedBy(rs.getString("updated_by"));
        datatable.setUpdatedAt(rs.getObject("updated_at") != null ? rs.getLong("updated_at") : null);
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
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }
}

