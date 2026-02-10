package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.entity.Application;
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
public class ApplicationDao extends GenericDaoImpl<Application, Identifier, String> {
    public static final String APPLICATION_ID = "id";

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    private IdGenerator idGenerator;

    @Override
    public Application create(@NotNull Application model, Identifier identifier) {
        return super.create(model, identifier);
    }

    @Override
    public Optional<Application> createV1(Application model, Identifier identifier) {
        try {
            if (model.getId() == null || model.getId().isEmpty()) {
                model.setId(idGenerator.generateId());
            }
            String applicationId = insertApplication(model, identifier);
            return getV1(new Identifier(applicationId));
        } catch (DataIntegrityViolationException e) {
            handleDataIntegrityViolationException(e);
        } catch (Exception e) {
            handleGenericException(e);
        }
        throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION);
    }

    @Override
    public Long insert(Application model, Identifier identifier) {
        return 0L;
    }

    public String insertApplication(Application model, Identifier identifier) {
        jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(getSql("Application.create"));
            ps.setObject(1, model.getId());
            ps.setObject(2, model.getName());
            ps.setObject(3, model.getEnvironment());
            ps.setObject(4, model.getDescription());
            ps.setObject(5, model.getCreatedBy() != null ? model.getCreatedBy() : "admin");
            ps.setObject(6, DateUtils.getUnixTimestampInUTC());
            return ps;
        });
        return model.getId();
    }

    @Override
    public Optional<Application> getV1(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(getSql("Application.getById"), applicationRowMapper,
                    identifier.getWord()));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    @Override
    public Optional<Application> getV1(Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    public int updateApp(Application application, Identifier identifier) {
        try {
            return jdbcTemplate.update(getSql("Application.updateById"),
                    application.getName(),
                    application.getEnvironment(),
                    application.getDescription(),
                    application.getUpdatedBy(),
                    DateUtils.getUnixTimestampInUTC(),
                    application.getId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public Optional<Application> updateV1(Application transientObject, Identifier identifier) {
        return Optional.empty();
    }

    @Override
    public Optional<Application> hotUpdate(Application transientObject, Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public List<Application> list(Identifier identifier) {
        try {
            return jdbcTemplate.query(getSql("Application.getAll"), applicationRowMapper);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    @Override
    public List<Application> list(Identifier identifier, String whereClause) {
        return List.of();
    }

    @Override
    public int count(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public int delete(Application application) {
        try {
            return jdbcTemplate.update(getSql("Application.deleteById"),
                    application.getUpdatedBy() != null ? application.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    DateUtils.getUnixTimestampInUTC(),
                    application.getId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public int delete(Identifier identifier, String whereClause) {
        return 0;
    }

    private void handleDataIntegrityViolationException(DataIntegrityViolationException e) {
        String errorMessage = e.getRootCause() != null ? e.getRootCause().getMessage() : e.getMessage();
        if (errorMessage != null) {
            if (errorMessage.contains(APPLICATION_ID)) {
                throw new DataloadersException(ErrorFactory.DUPLICATION, "An Application with this ID already exists");
            }
        }
        throw new DataloadersException(ErrorFactory.DUPLICATION, "An Application with this data already exists");
    }

    private void handleGenericException(Exception e) {
        logger.error("Exception Occurred: {}", e.getMessage());
        throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, e.getMessage());
    }

    RowMapper<Application> applicationRowMapper = (rs, rowNum) -> {
        Application application = new Application();
        application.setId(rs.getString("id"));
        application.setName(rs.getString("name"));
        application.setEnvironment(rs.getString("environment"));
        application.setDescription(rs.getString("description"));
        application.setCreatedBy(rs.getString("created_by"));
        application.setCreatedAt(rs.getObject("created_at") != null ? rs.getLong("created_at") : null);
        application.setUpdatedBy(rs.getString("updated_by"));
        application.setUpdatedAt(rs.getObject("updated_at") != null ? rs.getLong("updated_at") : null);
        return application;
    };
}