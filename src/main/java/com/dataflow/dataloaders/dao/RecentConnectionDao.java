package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.dto.RecentConnectionResponse;
import com.dataflow.dataloaders.entity.RecentConnection;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.DateUtils;
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
import java.util.Optional;
import java.util.Set;

@Slf4j
@Repository
public class RecentConnectionDao extends GenericDaoImpl<RecentConnection, Identifier, String> {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Override
    public Optional<RecentConnection> createV1(RecentConnection model, Identifier identifier) {
        try {
            Long id = insert(model, identifier);
            return getV1(Identifier.builder().id(id).build());
        } catch (Exception e) {
            log.error("Error creating recent connection: {}", e.getMessage());
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public Long insert(RecentConnection model, Identifier identifier) {
        KeyHolder holder = new GeneratedKeyHolder();
        jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(getSql("RecentConnection.create"), new String[] { "id" });
            // Note: SQL query uses ON CONFLICT to auto-update access_count and accessed_at
            ps.setObject(1, model.getUserId());
            ps.setObject(2, model.getConnectionId());
            ps.setObject(3, DateUtils.getUnixTimestampInUTC());
            ps.setInt(4, model.getAccessCount() != null ? model.getAccessCount() : 1);
            return ps;
        }, holder);

        // ON CONFLICT may not return a key if it's an update
        if (holder.getKey() != null) {
            return holder.getKey().longValue();
        }
        // If no key returned, it was an update - return the existing record's ID
        return model.getId();
    }

    @Override
    public Optional<RecentConnection> getV1(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("RecentConnection.getById"), recentConnectionRowMapper, identifier.getId()));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public List<RecentConnection> listByUserId(Long userId) {
        try {
            return jdbcTemplate.query(getSql("RecentConnection.getByUserId"), recentConnectionRowMapper, userId);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public List<RecentConnectionResponse> listByUserIdWithDetails(Long userId) {
        try {
            return jdbcTemplate.query(getSql("RecentConnection.getByUserId"), recentConnectionResponseRowMapper, userId);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    @Override
    public List<RecentConnection> list(Identifier identifier) {
        try {
            return jdbcTemplate.query(getSql("RecentConnection.getAll"), recentConnectionRowMapper);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public List<RecentConnectionResponse> listAllWithDetails(Identifier identifier) {
        try {
            return jdbcTemplate.query(getSql("RecentConnection.getAll"), recentConnectionResponseRowMapper);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    @Override
    public int delete(RecentConnection recentConnection) {
        try {
            return jdbcTemplate.update(getSql("RecentConnection.deleteById"), recentConnection.getId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public Optional<RecentConnection> getV1(Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public List<RecentConnection> list(Identifier identifier, String whereClause) {
        return List.of();
    }

    @Override
    public int count(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public Optional<RecentConnection> updateV1(RecentConnection transientObject, Identifier identifier) {
        return Optional.empty();
    }

    @Override
    public Optional<RecentConnection> hotUpdate(RecentConnection transientObject, Identifier identifier,
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

    RowMapper<RecentConnection> recentConnectionRowMapper = (rs, rowNum) -> RecentConnection.builder()
            .id(rs.getObject("id") != null ? rs.getLong("id") : null)
            .userId(rs.getObject("user_id") != null ? rs.getLong("user_id") : null)
            .connectionId(rs.getObject("connection_id") != null ? rs.getLong("connection_id") : null)
            .accessedAt(rs.getObject("accessed_at") != null ? rs.getLong("accessed_at") : null)
            .accessCount(rs.getObject("access_count") != null ? rs.getInt("access_count") : null)
            .build();

    RowMapper<RecentConnectionResponse> recentConnectionResponseRowMapper = (rs, rowNum) -> RecentConnectionResponse.builder()
            .id(rs.getObject("id") != null ? rs.getLong("id") : null)
            .userId(rs.getObject("user_id") != null ? rs.getLong("user_id") : null)
            .connectionId(rs.getObject("connection_id") != null ? rs.getLong("connection_id") : null)
            .connectionName(rs.getString("connection_name"))
            .providerDisplayName(rs.getString("provider_display_name"))
            .accessedAt(rs.getObject("accessed_at") != null ? rs.getLong("accessed_at") : null)
            .accessCount(rs.getObject("access_count") != null ? rs.getInt("access_count") : null)
            .build();
}
