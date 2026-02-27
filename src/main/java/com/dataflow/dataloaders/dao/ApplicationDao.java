package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.entity.Application;
import com.dataflow.dataloaders.enums.Visibility;
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
            handleDataIntegrityViolation(e, "Application");
        } catch (Exception e) {
            handleDatabaseException(e);
        }
        throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION);
    }

    @Override
    public Long insert(Application model, Identifier identifier) {
        return 0L;
    }

    public String insertApplication(Application model, Identifier identifier) {
        try {
            jdbcTemplate.update(con -> {
                PreparedStatement ps = con.prepareStatement(getSql("Application.create"));
                ps.setObject(1, model.getId());
                ps.setObject(2, model.getName());
                ps.setObject(3, model.getDescription());
                ps.setObject(4, model.getIconId());
                ps.setObject(5, model.getVisibility().name());
                ps.setObject(6, model.getCreatedBy() != null ? model.getCreatedBy() : "admin");
                ps.setObject(7, DateUtils.getUnixTimestampInUTC());
                return ps;
            });
            return model.getId();
        } catch (Exception e) {
            handleDatabaseException(e);
            return null;
        }
    }

    @Override
    public Optional<Application> getV1(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(getSql("Application.getById"), applicationRowMapper,
                    identifier.getWord()));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        } catch (Exception e) {
            handleDatabaseException(e);
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
                    application.getDescription(),
                    application.getIconId(),
                    application.getVisibility() != null ? application.getVisibility().name() : null,
                    application.getUpdatedBy(),
                    DateUtils.getUnixTimestampInUTC(),
                    application.getId());
        } catch (Exception e) {
            handleDatabaseException(e);
            return 0;
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

    public List<Application> list(Identifier identifier) {
        try {
            return jdbcTemplate.query(getSql("Application.getAll"), applicationRowMapper);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        } catch (Exception e) {
            handleDatabaseException(e);
            return List.of();
        }
    }

    public List<Application> listPaged(Identifier identifier) {
        try {
            return jdbcTemplate.query(getSql("Application.listPaged", identifier), applicationRowMapper);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        } catch (Exception e) {
            handleDatabaseException(e);
            return List.of();
        }
    }

    public List<Application> listByName(String name) {
        try {
            return jdbcTemplate.query(getSql("Application.searchByName"), applicationRowMapper, "%" + name + "%");
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        } catch (Exception e) {
            handleDatabaseException(e);
            return List.of();
        }
    }

    public List<Application> listByNamePaged(String name, Identifier identifier) {
        try {
            String sql = getSql("Application.searchByName", identifier);
            return jdbcTemplate.query(sql, applicationRowMapper, "%" + name + "%");
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        } catch (Exception e) {
            handleDatabaseException(e);
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
            handleDatabaseException(e);
            return 0;
        }
    }

    public int toggleFavorite(String applicationId, boolean isFavorite) {
        try {
            return jdbcTemplate.update(getSql("Application.toggleFavorite"),
                    isFavorite,
                    "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    applicationId);
        } catch (Exception e) {
            handleDatabaseException(e);
            return 0;
        }
    }

    @Override
    public int delete(Identifier identifier, String whereClause) {
        return 0;
    }

    RowMapper<Application> applicationRowMapper = (rs, rowNum) -> {
        Application application = new Application();
        application.setId(rs.getString("id"));
        application.setName(rs.getString("name"));
        application.setDescription(rs.getString("description"));
        application.setIconId(rs.getString("icon_id"));
        application.setVisibility(Visibility.valueOf(rs.getString("visibility")));
        application.setIsFavorite(rs.getObject("is_favorite") != null ? rs.getBoolean("is_favorite") : false);
        application.setCreatedBy(rs.getString("created_by"));
        application.setCreatedAt(rs.getObject("created_at", Long.class));
        application.setUpdatedBy(rs.getString("updated_by"));
        application.setUpdatedAt(rs.getObject("updated_at", Long.class));
        try {
            application.setTotal(rs.getObject("total") != null ? rs.getLong("total") : null);
        } catch (Exception ignored) {
            // 'total' column only present in paginated queries
        }
        return application;
    };
}