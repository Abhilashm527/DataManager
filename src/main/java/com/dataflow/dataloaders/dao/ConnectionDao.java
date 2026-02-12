package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.entity.Connection;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.DFUtil;
import com.dataflow.dataloaders.util.IdGenerator;
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
public class ConnectionDao extends GenericDaoImpl<Connection, Identifier, String> {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    private DFUtil dfUtil;

    @Autowired
    private IdGenerator idGenerator;

    @Override
    public Optional<Connection> createV1(Connection model, Identifier identifier) {
        try {
            if (model.getId() == null || model.getId().isEmpty()) {
                model.setId(idGenerator.generateId());
            }
            String id = insertConnection(model, identifier);
            return getV1(Identifier.builder().word(id).build());
        } catch (Exception e) {
            log.error("Error creating connection: {}", e.getMessage());
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public Long insert(Connection model, Identifier identifier) {
        return 0L;
    }

    public String insertConnection(Connection model, Identifier identifier) {
        KeyHolder holder = new GeneratedKeyHolder();
        jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(getSql("Connection.create"), new String[] { "id" });
            ps.setString(1, model.getId());
            ps.setObject(2, model.getApplicationId());
            ps.setObject(3, model.getProviderId());
            ps.setString(4, model.getConnectionName());
            ps.setObject(5, dfUtil.writeValueAsString(model.getConfig()), java.sql.Types.OTHER);
            ps.setObject(6, dfUtil.writeValueAsString(model.getSecrets()), java.sql.Types.OTHER);
            ps.setObject(7, model.getUseSsl());
            ps.setObject(8, model.getConnectionTimeout());
            ps.setObject(9, model.getIsActive());
            ps.setString(10, model.getLastTestStatus());
            ps.setObject(11, model.getLastTestedAt());
            ps.setObject(12, model.getLastUsedAt());
            ps.setObject(13, model.getIsFavorite() != null ? model.getIsFavorite() : false);
            ps.setObject(14, model.getCreatedBy() != null ? model.getCreatedBy() : "admin");
            ps.setObject(15, DateUtils.getUnixTimestampInUTC());
            return ps;
        }, holder);
        return model.getId();
    }

    @Override
    public Optional<Connection> getV1(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("Connection.getById"), connectionRowMapper, identifier.getWord()));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public List<Connection> listByUserId(String userId, Boolean isFavorite) {
        try {
            if (isFavorite != null) {
                return jdbcTemplate.query(getSql("Connection.getByUserIdAndFavorite"), connectionRowMapper, userId,
                        isFavorite);
            }
            return jdbcTemplate.query(getSql("Connection.getByUserId"), connectionRowMapper, userId);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public List<Connection> listByNameInApplication(String applicationId, String name) {
        try {
            return jdbcTemplate.query(getSql("Connection.searchByNameInApplication"), connectionRowMapper,
                    applicationId,
                    "%" + name + "%");
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public List<Connection> listByProvider(String providerId) {
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
                    connection.getLastUsedAt(),
                    connection.getIsFavorite(),
                    connection.getUpdatedBy() != null ? connection.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    connection.getId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public int delete(Connection connection) {
        try {
            return jdbcTemplate.update(getSql("Connection.deleteById"),
                    connection.getUpdatedBy() != null ? connection.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    DateUtils.getUnixTimestampInUTC(),
                    connection.getId());
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
    public <E extends Number> String setInvalues(String query, String replaceString, Set<E> inValues,
            String... delimitter) {
        return super.setInvalues(query, replaceString, inValues, delimitter);
    }

    @Override
    public String setInvalues(String query, String replaceString, Set<String> inValues) {
        return super.setInvalues(query, replaceString, inValues);
    }

    RowMapper<Connection> connectionRowMapper = (rs, rowNum) -> {
        Connection connection = new Connection();
        connection.setId(rs.getObject("id") != null ? rs.getString("id") : null);
        connection.setApplicationId(rs.getObject("application_id") != null ? rs.getString("application_id") : null);
        connection.setProviderId(rs.getObject("provider_id") != null ? rs.getString("provider_id") : null);
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
        connection.setConnectionTimeout(
                rs.getObject("connection_timeout") != null ? rs.getInt("connection_timeout") : null);
        connection.setIsActive(rs.getObject("is_active") != null ? rs.getBoolean("is_active") : null);
        connection.setLastTestStatus(rs.getString("last_test_status"));
        connection.setLastTestedAt(rs.getObject("last_tested_at") != null ? rs.getLong("last_tested_at") : null);
        connection.setCreatedAt(rs.getObject("created_at") != null ? rs.getLong("created_at") : null);
        connection.setCreatedBy(rs.getString("created_by"));
        connection.setUpdatedAt(rs.getObject("updated_at") != null ? rs.getLong("updated_at") : null);
        connection.setUpdatedBy(rs.getString("updated_by"));
        connection.setDeletedAt(rs.getObject("deleted_at") != null ? rs.getLong("deleted_at") : null);
        connection.setLastUsedAt(rs.getObject("last_used_at") != null ? rs.getLong("last_used_at") : null);
        connection.setIsFavorite(rs.getObject("is_favorite") != null ? rs.getBoolean("is_favorite") : null);
        return connection;
    };
}
