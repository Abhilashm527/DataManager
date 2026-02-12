package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.entity.SystemSetting;
import com.dataflow.dataloaders.util.DateUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Slf4j
@Repository
public class SystemSettingDao extends GenericDaoImpl<SystemSetting, String, String> {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    public Optional<SystemSetting> getByKey(String key) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("SystemSetting.getByKey"), systemSettingRowMapper, key));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public int update(SystemSetting setting) {
        try {
            return jdbcTemplate.update(getSql("SystemSetting.update"),
                    setting.getValue(),
                    setting.getUpdatedBy() != null ? setting.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    setting.getKey());
        } catch (Exception e) {
            log.error("Error updating system setting: {}", e.getMessage());
            return 0;
        }
    }

    private final RowMapper<SystemSetting> systemSettingRowMapper = (rs, rowNum) -> {
        SystemSetting setting = new SystemSetting();
        setting.setKey(rs.getString("key"));
        setting.setValue(rs.getString("value"));
        setting.setDescription(rs.getString("description"));
        setting.setUpdatedAt(rs.getLong("updated_at"));
        setting.setUpdatedBy(rs.getString("updated_by"));
        return setting;
    };

    @Override
    public Optional<SystemSetting> getV1(String key) {
        return getByKey(key);
    }

    @Override
    public Optional<SystemSetting> createV1(SystemSetting model, String identifier) {
        return Optional.empty();
    }

    @Override
    public Long insert(SystemSetting model, String identifier) {
        return 0L;
    }

    @Override
    public Optional<SystemSetting> getV1(String identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public List<SystemSetting> list(String identifier) {
        return List.of();
    }

    @Override
    public List<SystemSetting> list(String identifier, String whereClause) {
        return List.of();
    }

    @Override
    public int count(String identifier, String whereClause) {
        return 0;
    }

    @Override
    public Optional<SystemSetting> updateV1(SystemSetting transientObject, String identifier) {
        update(transientObject);
        return getByKey(transientObject.getKey());
    }

    @Override
    public Optional<SystemSetting> hotUpdate(SystemSetting transientObject, String identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public int delete(SystemSetting persistentObject) {
        return 0;
    }

    @Override
    public int delete(String identifier, String whereClause) {
        return 0;
    }
}
