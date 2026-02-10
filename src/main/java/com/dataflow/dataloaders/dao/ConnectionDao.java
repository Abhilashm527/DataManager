package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.entity.Connection;
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
public class ConnectionDao extends GenericDaoImpl<Connection, Identifier, String> {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    private DFUtil dfUtil;

    @Override
    public Optional<Connection> createV1(Connection model, Identifier identifier) {
        try {
            Long id = insert(model, identifier);
            return getV1(Identifier.builder().id(id).build());
        } catch (Exception e) {
            log.error("Error creating connection: {}", e.getMessage());
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public Long insert(Connection model, Identifier identifier) {
        KeyHolder holder = new GeneratedKeyHolder();
        jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(getSql("Connection.create"), new String[]{"id"});
            ps.setObject(1, model.getUserId());
            ps.setObject(2, model.getProviderId());
            ps.setString(3, model.getConnectionName());
            ps.setObject(4, dfUtil.writeValueAsString(model.getConfig()), java.sql.Types.OTHER);
            ps.setObject(5, dfUtil.writeValueAsString(model.getSecrets()), java.sql.Types.OTHER);
            ps.setObject(6, model.getUseSsl());
            ps.setObject(7, model.getConnectionTimeout());
            ps.setObject(8, model.getIsActive());
            ps.setString(9, model.getLastTestStatus());
            ps.setObject(10, model.getLastTestedAt());
            ps.setObject(11, DateUtils.getUnixTimestampInUTC());
            ps.setObject(12, DateUtils.getUnixTimestampInUTC());
            ps.setObject(13, model.getLastUsedAt());
            return ps;
        }, holder);
        return Objects.requireNonNull(holder.getKey()).longValue();
    }

    @Override
    public Optional<Connection> getV1(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("Connection.getById"), connectionRowMapper, identifier.getId()));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public List<Connection> listByUserId(Long userId) {
        try {
            return jdbcTemplate.query(getSql("Connection.getByUserId"), connectionRowMapper, userId);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public List<Connection> listByProvider(Long providerId) {
        try {
            return jdbcTemplate.query(getSql("Connection.getByProvider"), connectionRowMapper, providerId);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    @Override
    public List<Connection> list(Identifier identifier) {
        try {
            return jdbcTemplate.query(getSql("Connection.getAll"), connectionRowMapper);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public int update(Connection connection) {
        try {
            return jdbcTemplate.update(getSql("Connection.updateById"),
                    connection.getConnectionName(),
                    dfUtil.writeValueAsString(connection.getConfig()),
                    dfUtil.writeValueAsString(connection.getSecrets()),
                    connection.getUseSsl(),
                    connection.getConnectionTimeout(),
                    connection.getIsActive(),
                    connection.getLastTestStatus(),
                    connection.getLastTestedAt(),
                    DateUtils.getUnixTimestampInUTC(),
                    connection.getLastUsedAt(),
                    connection.getId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public int delete(Connection connection) {
        try {
            return jdbcTemplate.update(getSql("Connection.deleteById"), connection.getId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public Optional<Connection> getV1(Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public List<Connection> list(Identifier identifier, String whereClause) {
        return List.of();
    }

    @Override
    public int count(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public Optional<Connection> updateV1(Connection transientObject, Identifier identifier) {
        return Optional.empty();
    }

    @Override
    public Optional<Connection> hotUpdate(Connection transientObject, Identifier identifier, String whereClause) {
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

    RowMapper<Connection> connectionRowMapper = (rs, rowNum) -> {
        Connection connection = new Connection();
        connection.setId(rs.getObject("id") != null ? rs.getLong("id") : null);
        connection.setUserId(rs.getObject("user_id") != null ? rs.getLong("user_id") : null);
        connection.setProviderId(rs.getObject("provider_id") != null ? rs.getLong("provider_id") : null);
        connection.setConnectionName(rs.getString("connection_name"));
        
        String configJson = rs.getString("config");
        if (configJson != null) {
            connection.setConfig(dfUtil.readValueToJsonNode(configJson));
        }
        
        String secretsJson = rs.getString("secrets");
        if (secretsJson != null) {
            connection.setSecrets(dfUtil.readValueToJsonNode(secretsJson));
        }
        
        connection.setUseSsl(rs.getObject("use_ssl") != null ? rs.getBoolean("use_ssl") : null);
        connection.setConnectionTimeout(rs.getObject("connection_timeout") != null ? rs.getInt("connection_timeout") : null);
        connection.setIsActive(rs.getObject("is_active") != null ? rs.getBoolean("is_active") : null);
        connection.setLastTestStatus(rs.getString("last_test_status"));
        connection.setLastTestedAt(rs.getObject("last_tested_at") != null ? rs.getLong("last_tested_at") : null);
        connection.setCreatedAt(rs.getObject("created_at") != null ? rs.getLong("created_at") : null);
        connection.setUpdatedAt(rs.getObject("updated_at") != null ? rs.getLong("updated_at") : null);
        connection.setLastUsedAt(rs.getObject("last_used_at") != null ? rs.getLong("last_used_at") : null);
        return connection;
    };
}
