package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.entity.Deploy;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.IdGenerator;
import com.dataflow.dataloaders.util.Identifier;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

@Slf4j
@Repository
public class DeployDao extends GenericDaoImpl<Deploy, Identifier, String> {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    private IdGenerator idGenerator;

    @Override
    public Deploy create(@NotNull Deploy model, Identifier identifier) {
        return super.create(model, identifier);
    }

    @Override
    public Optional<Deploy> createV1(Deploy model, Identifier identifier) {
        try {
            if (model.getDeployId() == null || model.getDeployId().isEmpty()) {
                model.setDeployId(idGenerator.generateId());
            }
            String deployId = insertDeploy(model, identifier);
            return getV1(new Identifier(deployId));
        } catch (DataIntegrityViolationException e) {
            handleDataIntegrityViolationException(e);
        } catch (Exception e) {
            handleGenericException(e);
        }
        throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION);
    }

    public String insertDeploy(Deploy model, Identifier identifier) {
        jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(getSql("Deploy.create"));
            int idx = 1;
            ps.setObject(idx++, model.getDeployId());
            ps.setString(idx++, model.getDeployName());
            ps.setObject(idx++, model.getParentJobId());
            ps.setObject(idx++, model.getJobId());
            ps.setObject(idx++, model.isManualRun());
            ps.setObject(idx++, model.isSchedule());
            ps.setObject(idx++, model.getScheduleExpression());
            ps.setObject(idx++, model.getSchedulerId());
            ps.setObject(idx++, model.getSchedulerName());
            ps.setObject(idx++, model.isActive());
            ps.setObject(idx++, model.getCreatedBy());
            ps.setObject(idx++, model.getCreatedAt());
            return ps;
        });
        return model.getDeployId();
    }

    @Override
    public Long insert(Deploy model, Identifier identifier) {
        return 0L;
    }

    @Override
    public Optional<Deploy> getV1(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("Deploy.getById"),
                    deployRowMapper,
                    identifier.getWord()
            ));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public Optional<Deploy> getBySchedulerId(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("Deploy.getByScheduleId"),
                    deployRowMapper,
                    identifier.getId()
            ));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }




    @Override
    public Optional<Deploy> getV1(Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public List<Deploy> list(Identifier identifier) {
        try {
            return jdbcTemplate.query(getSql("Deploy.getAll"), deployRowMapper);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    @Override
    public List<Deploy> list(Identifier identifier, String whereClause) {
        return List.of();
    }

    @Override
    public int count(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public Optional<Deploy> updateV1(Deploy transientObject, Identifier identifier) {
        return Optional.empty();
    }

    @Override
    public Optional<Deploy> hotUpdate(Deploy transientObject, Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    public List<Deploy> getByJobId(Identifier identifier) {
        String sql = getSql("Deploy.getByJobId");
        return jdbcTemplate.query(sql, deployRowMapper, identifier.getWord());
    }

    public Optional<Deploy> getActiveByJobId(String jobId) {
        String sql = getSql("Deploy.getActiveByJobId");
        List<Deploy> results = jdbcTemplate.query(sql, deployRowMapper, jobId);
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }

    public int updateDeployStatus(Deploy deploy, Identifier identifier) {
        String sql = getSql("Deploy.updateStatus");
        return jdbcTemplate.update(sql, deploy.isActive(), deploy.getUpdatedBy(), 
                deploy.getUpdatedAt(), identifier.getWord());
    }

    @Override
    public int delete(Deploy deploy) {
        try {
            return jdbcTemplate.update(getSql("Deploy.deleteById"),
                    deploy.getUpdatedBy() != null ? deploy.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    DateUtils.getUnixTimestampInUTC(),
                    deploy.getDeployId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    @Override
    public int delete(Identifier identifier, String whereClause) {
        return 0;
    }

    private void handleDataIntegrityViolationException(DataIntegrityViolationException e) {
        String errorMessage = e.getRootCause() != null ? e.getRootCause().getMessage() : e.getMessage();
        log.error("DataIntegrityViolationException: {}", errorMessage);
        throw new DataloadersException(ErrorFactory.DUPLICATION, "A Deploy with this data already exists");
    }

    private void handleGenericException(Exception e) {
        log.error("Exception Occurred: {}", e.getMessage());
        throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, e.getMessage());
    }

    RowMapper<Deploy> deployRowMapper = (rs, rowNum) -> {
        Deploy deploy = new Deploy();
        deploy.setDeployId(rs.getString("deploy_id"));
        deploy.setDeployName(rs.getString("deploy_name"));
        deploy.setParentJobId(rs.getString("parent_id"));
        deploy.setJobId(rs.getString("job_id"));
        deploy.setManualRun(rs.getBoolean("manual_run"));
        deploy.setSchedule(rs.getBoolean("schedule"));
        deploy.setScheduleExpression(rs.getString("schedule_expression"));
        deploy.setSchedulerId(rs.getLong("scheduler_id"));
        deploy.setActive(rs.getBoolean("active"));
        deploy.setCreatedBy(rs.getString("created_by"));
        deploy.setCreatedAt(rs.getLong("created_at"));
        deploy.setUpdatedBy(rs.getString("updated_by"));
        deploy.setUpdatedAt(rs.getLong("updated_at"));
        return deploy;
    };

    public List<Deploy> getAllDeployByappId(Identifier identifier) {
        try {
            return jdbcTemplate.query(getSql("Deploy.getByAppId"), deployRowMapper,identifier.getWord());
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public int deleteBySheduleId(Identifier identifier) {
        try {
            return jdbcTemplate.update(getSql("Deploy.deleteByScheduleId"),
                    DateUtils.getUnixTimestampInUTC(),
                    DateUtils.getUnixTimestampInUTC(),
                    identifier.getId());
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }
}