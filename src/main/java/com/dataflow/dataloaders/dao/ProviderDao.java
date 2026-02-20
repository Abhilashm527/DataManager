package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.entity.Provider;
import com.dataflow.dataloaders.entity.ConfigSchema;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@Slf4j
@Repository
public class ProviderDao extends GenericDaoImpl<Provider, Identifier, String> {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    private DFUtil dfUtil;
    @Autowired
    private IdGenerator idGenerator;

    @Override
    public Optional<Provider> createV1(Provider model, Identifier identifier) {
        try {
            if (model.getId() == null || model.getId().isEmpty()) {
                model.setId(idGenerator.generateId());
            }
            String id = insertProvider(model, identifier);
            return getV1(Identifier.builder().word(id).build());
        } catch (Exception e) {
            log.error("Error creating provider: {}", e.getMessage());
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public Long insert(Provider model, Identifier identifier) {
        return 0L;
    }

    public String insertProvider(Provider model, Identifier identifier) {
        KeyHolder holder = new GeneratedKeyHolder();
        jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(getSql("Provider.create"), new String[] { "id" });
            ps.setObject(1, model.getId());
            ps.setObject(2, model.getConnectionTypeId());
            ps.setString(3, model.getProviderName());
            ps.setString(4, model.getDescription());
            ps.setObject(5, model.getIconId());
            ps.setObject(6, model.getDefaultPort());
            ps.setObject(7, dfUtil.writeValueAsString(model.getConfigSchema()), java.sql.Types.OTHER);
            ps.setObject(8, model.getDisplayOrder());
            ps.setObject(9, model.getCreatedBy() != null ? model.getCreatedBy() : "admin");
            ps.setObject(10, DateUtils.getUnixTimestampInUTC());
            return ps;
        }, holder);
        return model.getId();
    }

    @Override
    public Optional<Provider> getV1(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("Provider.getById"), providerRowMapper, identifier.getWord()));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public Optional<Provider> getByProviderKey(String providerKey) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("Provider.getByProviderName"), providerRowMapper, providerKey));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public List<Provider> listByConnectionType(String connectionTypeId) {
        try {
            return jdbcTemplate.query(getSql("Provider.getByConnectionType"), providerRowMapper, connectionTypeId);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    @Override
    public List<Provider> list(Identifier identifier) {
        try {
            return jdbcTemplate.query(getSql("Provider.getAll"), providerRowMapper);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public int update(Provider provider) {
        try {
            return jdbcTemplate.update(getSql("Provider.updateById"),
                    provider.getIconId(),
                    provider.getDefaultPort(),
                    dfUtil.writeValueAsString(provider.getConfigSchema()),
                    provider.getDisplayOrder(),
                    provider.getUpdatedBy() != null ? provider.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    provider.getId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public int delete(Provider provider) {
        try {
            return jdbcTemplate.update(getSql("Provider.deleteById"),
                    provider.getUpdatedBy() != null ? provider.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    DateUtils.getUnixTimestampInUTC(),
                    provider.getId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public Optional<Provider> getV1(Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public List<Provider> list(Identifier identifier, String whereClause) {
        return List.of();
    }

    @Override
    public int count(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public Optional<Provider> updateV1(Provider transientObject, Identifier identifier) {
        return Optional.empty();
    }

    @Override
    public Optional<Provider> hotUpdate(Provider transientObject, Identifier identifier, String whereClause) {
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

    RowMapper<Provider> providerRowMapper = (rs, rowNum) -> {
        Provider provider = new Provider();
        provider.setId(rs.getString("id"));
        provider.setConnectionTypeId(rs.getString("connection_type_id"));
        provider.setProviderName(rs.getString("provider_name"));
        provider.setDescription(rs.getString("description"));
        provider.setIconId(rs.getString("icon_id") != null ? rs.getString("icon_id") : null);
        provider.setDefaultPort(rs.getObject("default_port") != null ? rs.getInt("default_port") : null);
        String configSchemaJson = rs.getString("config_schema");
        if (configSchemaJson != null) {
            provider.setConfigSchema(dfUtil.readValue(ConfigSchema.class, configSchemaJson));
        }
        provider.setDisplayOrder(rs.getObject("display_order") != null ? rs.getInt("display_order") : null);
        provider.setCreatedAt(rs.getObject("created_at") != null ? rs.getLong("created_at") : null);
        provider.setCreatedBy(rs.getString("created_by"));
        provider.setUpdatedAt(rs.getObject("updated_at") != null ? rs.getLong("updated_at") : null);
        provider.setUpdatedBy(rs.getString("updated_by"));
        provider.setDeletedAt(rs.getObject("deleted_at") != null ? rs.getLong("deleted_at") : null);
        return provider;
    };
}
