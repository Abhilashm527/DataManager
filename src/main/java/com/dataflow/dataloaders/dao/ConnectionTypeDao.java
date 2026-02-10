package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.entity.ConnectionType;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@Slf4j
@Repository
public class ConnectionTypeDao extends GenericDaoImpl<ConnectionType, Identifier, String> {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Override
    public Optional<ConnectionType> createV1(ConnectionType model, Identifier identifier) {
        try {
            Long id = insert(model, identifier);
            return getV1(Identifier.builder().id(id).build());
        } catch (Exception e) {
            log.error("Error creating connection type: {}", e.getMessage());
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public Long insert(ConnectionType model, Identifier identifier) {
        KeyHolder holder = new GeneratedKeyHolder();
        jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(getSql("ConnectionType.create"), new String[]{"id"});
            ps.setString(1, model.getTypeKey());
            ps.setString(2, model.getDisplayName());
            ps.setObject(3, model.getIconId());
            ps.setObject(4, model.getDisplayOrder());
            ps.setObject(5, DateUtils.getUnixTimestampInUTC());
            return ps;
        }, holder);
        return Objects.requireNonNull(holder.getKey()).longValue();
    }

    @Override
    public Optional<ConnectionType> getV1(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("ConnectionType.getById"), connectionTypeRowMapper, identifier.getId()));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public Optional<ConnectionType> getByTypeKey(String typeKey) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("ConnectionType.getByTypeKey"), connectionTypeRowMapper, typeKey));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    @Override
    public List<ConnectionType> list(Identifier identifier) {
        try {
            return jdbcTemplate.query(getSql("ConnectionType.getAll"), connectionTypeRowMapper);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public int update(ConnectionType connectionType) {
        try {
            return jdbcTemplate.update(getSql("ConnectionType.updateById"),
                    connectionType.getDisplayName(),
                    connectionType.getIconId(),
                    connectionType.getDisplayOrder(),
                    connectionType.getId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public int delete(ConnectionType connectionType) {
        try {
            return jdbcTemplate.update(getSql("ConnectionType.deleteById"), connectionType.getId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public Optional<ConnectionType> getV1(Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public List<ConnectionType> list(Identifier identifier, String whereClause) {
        return List.of();
    }

    @Override
    public int count(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public Optional<ConnectionType> updateV1(ConnectionType transientObject, Identifier identifier) {
        return Optional.empty();
    }

    @Override
    public Optional<ConnectionType> hotUpdate(ConnectionType transientObject, Identifier identifier, String whereClause) {
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

    RowMapper<ConnectionType> connectionTypeRowMapper = (rs, rowNum) -> ConnectionType.builder()
            .id(rs.getObject("id") != null ? rs.getLong("id") : null)
            .typeKey(rs.getString("type_key"))
            .displayName(rs.getString("display_name"))
            .iconId(rs.getObject("icon_id") != null ? rs.getLong("icon_id") : null)
            .displayOrder(rs.getObject("display_order") != null ? rs.getInt("display_order") : null)
            .createdAt(rs.getObject("created_at") != null ? rs.getLong("created_at") : null)
            .build();
}
