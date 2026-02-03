package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.entity.ActivityLog;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.Identifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.Set;

@Slf4j
@Repository
public class ActivityLogDao extends GenericDaoImpl<ActivityLog, Identifier, String> {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Override
    public ActivityLog create(ActivityLog model, Identifier identifier) {
        return super.create(model, identifier);
    }

    @Override
    public Optional<ActivityLog> createV1(ActivityLog model, Identifier identifier) {
        try {
            jdbcTemplate.update(getSql("ActivityLog.create"),
                    model.getId(),
                    model.getActivityType(),
                    model.getEntityType(),
                    model.getEntityId(),
                    model.getEntityName(),
                    model.getAction(),
                    model.getDescription(),
                    model.getUserId(),
                    model.getCreatedAt());
            
            return getV1(new Identifier(model.getId()));
        } catch (Exception e) {
            log.error("Error creating ActivityLog", e);
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public Optional<ActivityLog> getV1(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("ActivityLog.getById"),
                    activityLogRowMapper,
                    identifier.getWord()
            ));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    @Override
    public ActivityLog get(Identifier identifier, String whereClause) {
        return super.get(identifier, whereClause);
    }

    @Override
    public Optional<ActivityLog> getV1(Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public List<ActivityLog> list(Identifier identifier) {
        try {
            return jdbcTemplate.query(getSql("ActivityLog.getAll"), activityLogRowMapper);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    @Override
    public List<ActivityLog> list(Identifier identifier, String whereClause) {
        return List.of();
    }

    @Override
    public int count(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public ActivityLog update(ActivityLog transientObject, Identifier identifier) {
        return super.update(transientObject, identifier);
    }

    @Override
    public Optional<ActivityLog> updateV1(ActivityLog transientObject, Identifier identifier) {
        return Optional.empty();
    }

    @Override
    public Optional<ActivityLog> hotUpdate(ActivityLog transientObject, Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public int delete(ActivityLog persistentObject) {
        return 0;
    }

    @Override
    public int deleteV1(Optional<ActivityLog> persistentObject) {
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

    public List<ActivityLog> getRecentActivities(int limit) {
        try {
            return jdbcTemplate.query(getSql("ActivityLog.getRecent"), activityLogRowMapper, limit);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    RowMapper<ActivityLog> activityLogRowMapper = (rs, rowNum) -> {
        ActivityLog activity = new ActivityLog();
        activity.setId(rs.getString("id"));
        activity.setActivityType(rs.getString("activity_type"));
        activity.setEntityType(rs.getString("entity_type"));
        activity.setEntityId(rs.getString("entity_id"));
        activity.setEntityName(rs.getString("entity_name"));
        activity.setAction(rs.getString("action"));
        activity.setDescription(rs.getString("description"));
        activity.setUserId(rs.getString("user_id"));
        activity.setCreatedAt(rs.getLong("created_at"));
        return activity;
    };

    @Override
    public Long insert(ActivityLog model, Identifier identifier) {
        return 0L;
    }

    @Override
    public ActivityLog upsert(ActivityLog model, Identifier identifier) {
        return super.upsert(model, identifier);
    }

    @Override
    public ActivityLog upsert(ActivityLog model, Identifier identifier, String whereClause) {
        return super.upsert(model, identifier, whereClause);
    }

    @Override
    public ActivityLog get(Identifier identifier) {
        return super.get(identifier);
    }
}