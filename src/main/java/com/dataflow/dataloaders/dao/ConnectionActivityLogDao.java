package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.entity.ConnectionActivityLog;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.DFUtil;
import com.dataflow.dataloaders.util.Identifier;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@Slf4j
@Repository
public class ConnectionActivityLogDao extends GenericDaoImpl<ConnectionActivityLog, Identifier, String> {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    private DFUtil dfUtil;

    @Override
    public Optional<ConnectionActivityLog> createV1(ConnectionActivityLog model, Identifier identifier) {
        try {
            Long id = insert(model, identifier);
            return getV1(Identifier.builder().id(id).build());
        } catch (Exception e) {
            log.error("Error creating connection activity log: {}", e.getMessage());
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public Long insert(ConnectionActivityLog model, Identifier identifier) {
        KeyHolder holder = new GeneratedKeyHolder();
        jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(getSql("ConnectionActivityLog.create"), new String[]{"id"});
            ps.setObject(1, model.getConnectionId());
            ps.setString(2, model.getActivityType());
            ps.setString(3, model.getStatus());
            ps.setString(4, model.getTitle());
            ps.setString(5, model.getDescription());
            ps.setObject(6, dfUtil.writeValueAsString(model.getMetadata()), java.sql.Types.OTHER);
            ps.setObject(7, model.getCreatedBy() != null ? model.getCreatedBy() : "admin");
            ps.setObject(8, DateUtils.getUnixTimestampInUTC());
            return ps;
        }, holder);
        return Objects.requireNonNull(holder.getKey()).longValue();
    }

    @Override
    public Optional<ConnectionActivityLog> getV1(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("ConnectionActivityLog.getById"), activityLogRowMapper, identifier.getId()));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public List<ConnectionActivityLog> listByConnectionId(Long connectionId) {
        try {
            return jdbcTemplate.query(getSql("ConnectionActivityLog.getByConnectionId"), activityLogRowMapper, connectionId);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public List<ConnectionActivityLog> listByActivityType(Long connectionId, String activityType) {
        try {
            return jdbcTemplate.query(getSql("ConnectionActivityLog.getByActivityType"), activityLogRowMapper, connectionId, activityType);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    @Override
    public List<ConnectionActivityLog> list(Identifier identifier) {
        try {
            return jdbcTemplate.query(getSql("ConnectionActivityLog.getAll"), activityLogRowMapper);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    @Override
    public int delete(ConnectionActivityLog activityLog) {
        try {
            return jdbcTemplate.update(getSql("ConnectionActivityLog.deleteById"),
                    activityLog.getUpdatedBy() != null ? activityLog.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    DateUtils.getUnixTimestampInUTC(),
                    activityLog.getId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public Optional<ConnectionActivityLog> getV1(Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public List<ConnectionActivityLog> list(Identifier identifier, String whereClause) {
        return List.of();
    }

    @Override
    public int count(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public Optional<ConnectionActivityLog> updateV1(ConnectionActivityLog transientObject, Identifier identifier) {
        return Optional.empty();
    }

    @Override
    public Optional<ConnectionActivityLog> hotUpdate(ConnectionActivityLog transientObject, Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public int delete(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public <E extends Number> String setInvalues(String query, String replaceString, Set<E> inValues, String... delimitter) {
        return super.setInvalues(query, replaceString, inValues, delimitter);
    }

    @Override
    public String setInvalues(String query, String replaceString, Set<String> inValues) {
        return super.setInvalues(query, replaceString, inValues);
    }

    RowMapper<ConnectionActivityLog> activityLogRowMapper = (rs, rowNum) -> {
        ConnectionActivityLog log = new ConnectionActivityLog();
        log.setId(rs.getObject("id") != null ? rs.getLong("id") : null);
        log.setConnectionId(rs.getObject("connection_id") != null ? rs.getLong("connection_id") : null);
        log.setActivityType(rs.getString("activity_type"));
        log.setStatus(rs.getString("status"));
        log.setTitle(rs.getString("title"));
        log.setDescription(rs.getString("description"));
        
        String metadataJson = rs.getString("metadata");
        if (metadataJson != null) {
            log.setMetadata(dfUtil.readValueToJsonNode(metadataJson));
        }
        
        log.setCreatedAt(rs.getObject("created_at") != null ? rs.getLong("created_at") : null);
        log.setCreatedBy(rs.getString("created_by"));
        log.setUpdatedAt(rs.getObject("updated_at") != null ? rs.getLong("updated_at") : null);
        log.setUpdatedBy(rs.getString("updated_by"));
        log.setDeletedAt(rs.getObject("deleted_at") != null ? rs.getLong("deleted_at") : null);
        return log;
    };
}
