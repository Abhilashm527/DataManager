package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.entity.Icon;
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
public class IconDao extends GenericDaoImpl<Icon, Identifier, String> {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Override
    public Optional<Icon> createV1(Icon model, Identifier identifier) {
        try {
            Long id = insert(model, identifier);
            return getV1(Identifier.builder().id(id).build());
        } catch (Exception e) {
            log.error("Error creating icon: {}", e.getMessage());
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public Long insert(Icon model, Identifier identifier) {
        KeyHolder holder = new GeneratedKeyHolder();
        jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(getSql("Icon.create"), new String[] { "id" });
            ps.setString(1, model.getIconName());
            ps.setString(2, model.getIconUrl());
            ps.setBytes(3, model.getIconData()); // NEW: Binary data
            ps.setString(4, model.getContentType()); // NEW: MIME type
            ps.setObject(5, model.getFileSize()); // NEW: File size
            ps.setObject(6, DateUtils.getUnixTimestampInUTC());
            return ps;
        }, holder);
        return Objects.requireNonNull(holder.getKey()).longValue();
    }

    @Override
    public Optional<Icon> getV1(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("Icon.getById"), iconRowMapper, identifier.getId()));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public Optional<Icon> getByName(String iconName) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("Icon.getByName"), iconRowMapper, iconName));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    @Override
    public List<Icon> list(Identifier identifier) {
        try {
            return jdbcTemplate.query(getSql("Icon.getAll"), iconRowMapper);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public int update(Icon icon) {
        try {
            return jdbcTemplate.update(getSql("Icon.updateById"),
                    icon.getIconName(),
                    icon.getIconUrl(),
                    icon.getIconData(), // NEW
                    icon.getContentType(), // NEW
                    icon.getFileSize(), // NEW
                    icon.getId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public int delete(Icon icon) {
        try {
            return jdbcTemplate.update(getSql("Icon.deleteById"), icon.getId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public Optional<Icon> getV1(Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public List<Icon> list(Identifier identifier, String whereClause) {
        return List.of();
    }

    @Override
    public int count(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public Optional<Icon> updateV1(Icon transientObject, Identifier identifier) {
        return Optional.empty();
    }

    @Override
    public Optional<Icon> hotUpdate(Icon transientObject, Identifier identifier, String whereClause) {
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

    RowMapper<Icon> iconRowMapper = (rs, rowNum) -> Icon.builder()
            .id(rs.getObject("id") != null ? rs.getLong("id") : null)
            .iconName(rs.getString("icon_name"))
            .iconUrl(rs.getString("icon_url"))
            .iconData(rs.getBytes("icon_data")) // NEW
            .contentType(rs.getString("content_type")) // NEW
            .fileSize(rs.getObject("file_size") != null ? rs.getLong("file_size") : null) // NEW
            .createdAt(rs.getObject("created_at") != null ? rs.getLong("created_at") : null)
            .build();
}
