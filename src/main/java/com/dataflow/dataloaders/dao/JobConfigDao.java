package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.dto.SourceConfig;
import com.dataflow.dataloaders.dto.TargetConfig;
import com.dataflow.dataloaders.entity.JobConfig;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.IdGenerator;
import com.dataflow.dataloaders.util.Identifier;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Slf4j
@Repository
public class JobConfigDao extends GenericDaoImpl<JobConfig, Identifier, String> {

    public static final String JOB_CONFIG_ID_PK = "job_configs_pkey";
    public static final String UNIQUE_JOB_NAME = "unique_job_name_per_item";

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    private IdGenerator idGenerator;

    @Override
    public JobConfig create(@NotNull JobConfig model, Identifier identifier) {
        return super.create(model, identifier);
    }

    @Override
    public Optional<JobConfig> createV1(JobConfig model, Identifier identifier) {
        try {
            // Always generate new ID for new records
            if (model.getJobId() == null || model.getJobId().isEmpty()) {
                model.setJobId(idGenerator.generateId());
            }
            String jobId = insertJobConfig(model, identifier);
            return getV1(new Identifier(jobId));
        } catch (DataIntegrityViolationException e) {
            handleDataIntegrityViolationException(e);
        } catch (Exception e) {
            handleGenericException(e);
        }
        throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION);
    }

    public String insertJobConfig(JobConfig model, Identifier identifier) {
        jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    getSql("JobConfig.create"));
            int idx = 1;
            ps.setObject(idx++, model.getJobId());
            ps.setObject(idx++, model.getParentJobId());
            ps.setObject(idx++, model.getJobName());
            ps.setObject(idx++, model.getJobDescription());
            ps.setObject(idx++, model.getImpacts());
            ps.setObject(idx++, model.getJobSeverity());
            ps.setObject(idx++, model.getChunkSize());
            ps.setObject(idx++, model.getMappingId());
            ps.setObject(idx++, toJsonSourceConfig(model.getSourceConfig()));
            ps.setObject(idx++, toJsonTargetConfig(model.getTargetConfig()));
            ps.setObject(idx++, model.getScheduled());
            ps.setObject(idx++, model.getSchedule());
            ps.setObject(idx++, model.getPublished());
            ps.setObject(idx++, model.getPublishedVersion());
            ps.setObject(idx++, model.getDeployedVersion());
            ps.setObject(idx++, model.getDeployed());
            ps.setObject(idx++, model.getStatus());
            ps.setObject(idx++, model.getIsActive());
            ps.setObject(idx++, model.getCreatedBy());
            ps.setObject(idx++, model.getCreatedAt());
            ps.setObject(idx++, null); // updated_by
            ps.setObject(idx++, null); // updated_at
            ps.setObject(idx++, null); // deleted_at
            ps.setObject(idx++, model.getItemId());
            ps.setBoolean(idx++, model.getDrafted() != null ? model.getDrafted() : false);

            return ps;
        });

        return model.getJobId();
    }

    @Override
    public Long insert(JobConfig model, Identifier identifier) {
        return 0L;
    }

    @Override
    public Optional<JobConfig> getV1(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("JobConfig.getById"),
                    jobConfigRowMapper,
                    identifier.getWord()));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public Optional<JobConfig> getLatestByParentId(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("JobConfig.getLatestByParentId"),
                    jobConfigRowMapper,
                    identifier.getWord()));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public int updateJobConfig(JobConfig jobConfig, Identifier identifier) {
        try {
            return jdbcTemplate.update(getSql("JobConfig.updateById"),
                    jobConfig.getParentJobId(),
                    jobConfig.getJobName(),
                    jobConfig.getJobDescription(),
                    jobConfig.getImpacts(),
                    jobConfig.getJobSeverity(),
                    jobConfig.getChunkSize(),
                    jobConfig.getMappingId(),
                    toJsonSourceConfig(jobConfig.getSourceConfig()),
                    toJsonTargetConfig(jobConfig.getTargetConfig()),
                    jobConfig.getScheduled(),
                    jobConfig.getSchedule(),
                    jobConfig.getPublished(),
                    jobConfig.getPublishedVersion(),
                    jobConfig.getDeployedVersion(),
                    jobConfig.getDeployed(),
                    jobConfig.getStatus(),
                    jobConfig.getIsActive(),
                    jobConfig.getUpdatedBy(),
                    DateUtils.getUnixTimestampInUTC(),
                    jobConfig.getJobId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    public int updateJobConfigParentJobId(JobConfig jobConfig, Identifier identifier) {
        try {
            return jdbcTemplate.update(getSql("JobConfig.updateParentJobId"),
                    jobConfig.getParentJobId(),
                    jobConfig.getUpdatedBy(),
                    DateUtils.getUnixTimestampInUTC(),
                    jobConfig.getJobId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public List<JobConfig> list(Identifier identifier) {
        try {
            return jdbcTemplate.query(getSql("JobConfig.getAll"), jobConfigRowMapper);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public List<JobConfig> getByParentId(Identifier identifier) {
        try {
            return jdbcTemplate.query(getSql("JobConfig.getByParentId"), jobConfigRowMapper, identifier.getWord());
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    @Override
    public int delete(JobConfig jobConfig) {
        try {
            return jdbcTemplate.update(getSql("JobConfig.deleteById"),
                    jobConfig.getUpdatedBy() != null ? jobConfig.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    DateUtils.getUnixTimestampInUTC(),
                    jobConfig.getJobId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    public int deleteByParentId(Identifier identifier) {
        try {
            return jdbcTemplate.update(getSql("JobConfig.deleteByParentId"),
                    "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    DateUtils.getUnixTimestampInUTC(),
                    identifier.getWord());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    // Helper methods

    private String toJsonSourceConfig(SourceConfig sourceConfig) {
        if (sourceConfig == null) {
            return "{}";
        }
        try {
            return objectMapper.writeValueAsString(sourceConfig);
        } catch (JsonProcessingException e) {
            log.error("Error converting SourceConfig to JSON", e);
            return "{}";
        }
    }

    private String toJsonTargetConfig(TargetConfig targetConfig) {
        if (targetConfig == null) {
            return "{}";
        }
        try {
            return objectMapper.writeValueAsString(targetConfig);
        } catch (JsonProcessingException e) {
            log.error("Error converting TargetConfig to JSON", e);
            return "{}";
        }
    }

    private SourceConfig fromJsonSourceConfig(String json) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        try {
            return objectMapper.readValue(json, SourceConfig.class);
        } catch (JsonProcessingException e) {
            log.error("Error parsing JSON to SourceConfig", e);
            return null;
        }
    }

    private TargetConfig fromJsonTargetConfig(String json) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        try {
            return objectMapper.readValue(json, TargetConfig.class);
        } catch (JsonProcessingException e) {
            log.error("Error parsing JSON to TargetConfig", e);
            return null;
        }
    }

    private void handleDataIntegrityViolationException(DataIntegrityViolationException e) {
        String errorMessage = e.getRootCause() != null ? e.getRootCause().getMessage() : e.getMessage();
        log.error("DataIntegrityViolationException: {}", errorMessage);

        if (errorMessage != null) {
            if (errorMessage.contains(JOB_CONFIG_ID_PK) || errorMessage.contains("job_id")) {
                throw new DataloadersException(ErrorFactory.DUPLICATION,
                        "A JobConfig with this ID already exists");
            }
            if (errorMessage.contains(UNIQUE_JOB_NAME) || errorMessage.contains("job_name")) {
                throw new DataloadersException(ErrorFactory.DUPLICATION,
                        "A JobConfig with this name already exists in this application");
            }
        }
        throw new DataloadersException(ErrorFactory.DUPLICATION,
                "A JobConfig with this data already exists");
    }

    private void handleGenericException(Exception e) {
        log.error("Exception Occurred: {}", e.getMessage());
        throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, e.getMessage());
    }

    public Optional<JobConfig> getPublishedVersion(String parentJobId, String version) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("JobConfig.getPublishedVersion"),
                    jobConfigRowMapper,
                    parentJobId, version));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public Optional<String> getLatestPublishedVersion(String parentJobId) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("JobConfig.getLatestPublishedVersion"),
                    String.class,
                    parentJobId));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public Optional<String> getLatestDeployedVersion(String parentJobId) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("JobConfig.getLatestDeployedVersion"),
                    String.class,
                    parentJobId));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    // Row Mapper
    RowMapper<JobConfig> jobConfigRowMapper = (rs, rowNum) -> {
        JobConfig jobConfig = new JobConfig();

        jobConfig.setJobId(rs.getString("job_id"));
        jobConfig.setParentJobId(rs.getString("parent_job_id"));
        jobConfig.setJobName(rs.getString("job_name"));
        jobConfig.setJobDescription(rs.getString("job_description"));
        jobConfig.setImpacts(rs.getString("impacts"));
        jobConfig.setJobSeverity(rs.getString("job_severity"));
        jobConfig.setChunkSize(rs.getInt("chunk_size"));
        jobConfig.setMappingId(rs.getString("mapping_id"));

        jobConfig.setSourceConfig(fromJsonSourceConfig(rs.getString("source_config")));
        jobConfig.setTargetConfig(fromJsonTargetConfig(rs.getString("target_config")));

        jobConfig.setScheduled(rs.getBoolean("scheduled"));
        jobConfig.setSchedule(rs.getString("schedule"));
        jobConfig.setPublished(rs.getBoolean("published"));
        jobConfig.setPublishedVersion(rs.getString("published_version"));
        jobConfig.setDeployedVersion(rs.getString("deployed_version"));
        jobConfig.setDeployed(rs.getBoolean("deployed"));
        jobConfig.setStatus(rs.getString("status"));
        jobConfig.setIsActive(rs.getBoolean("is_active"));
        jobConfig.setItemId(rs.getString("item_id"));
        jobConfig.setDrafted(rs.getBoolean("drafted"));
        // Audit fields
        jobConfig.setCreatedBy(rs.getString("created_by"));
        jobConfig.setCreatedAt(rs.getLong("created_at"));
        jobConfig.setUpdatedBy(rs.getString("updated_by"));
        jobConfig.setUpdatedAt(rs.getLong("updated_at"));

        return jobConfig;
    };

    // Unused overrides from GenericDaoImpl
    @Override
    public JobConfig upsert(JobConfig model, Identifier identifier) {
        return super.upsert(model, identifier);
    }

    @Override
    public JobConfig upsert(JobConfig model, Identifier identifier, String whereClause) {
        return super.upsert(model, identifier, whereClause);
    }

    @Override
    public JobConfig get(@NotNull Identifier identifier) {
        return super.get(identifier);
    }

    @Override
    public JobConfig get(Identifier identifier, String whereClause) {
        return super.get(identifier, whereClause);
    }

    @Override
    public Optional<JobConfig> getV1(Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public List<JobConfig> list(Identifier identifier, String whereClause) {
        return List.of();
    }

    @Override
    public int count(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public JobConfig update(JobConfig transientObject, Identifier identifier) {
        return super.update(transientObject, identifier);
    }

    @Override
    public Optional<JobConfig> updateV1(JobConfig transientObject, Identifier identifier) {
        return Optional.empty();
    }

    @Override
    public Optional<JobConfig> hotUpdate(JobConfig transientObject, Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public int deleteV1(Optional<JobConfig> persistentObject) {
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
    public <E extends Number> String setInvalues(String query, String replaceString, Set<E> inValues,
            String... delimitter) {
        return super.setInvalues(query, replaceString, inValues, delimitter);
    }

    @Override
    public String setInvalues(String query, String replaceString, Set<String> inValues) {
        return super.setInvalues(query, replaceString, inValues);
    }

    public List<java.util.Map<String, Object>> searchByNameOrDescription(String query, Identifier identifier) {
        try {
            String sql = "SELECT job_id, job_name, job_description, status FROM job_configs " +
                    "WHERE (LOWER(job_name) LIKE LOWER(?) OR LOWER(job_description) LIKE LOWER(?)) " +
                    "AND deleted_at IS NULL ORDER BY job_name";
            String searchPattern = "%" + query + "%";
            return jdbcTemplate.queryForList(sql, searchPattern, searchPattern);
        } catch (Exception e) {
            log.error("Error searching job configs: {}", e.getMessage());
            return List.of();
        }
    }

}