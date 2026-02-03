package com.dataflow.dataloaders.dao;

import com.dataflow.dataloaders.entity.JobConfigReference;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.IdGenerator;
import com.dataflow.dataloaders.util.Identifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.Set;

@Slf4j
@Repository
public class JobConfigReferenceDao extends GenericDaoImpl<JobConfigReference, Identifier, String> {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    private IdGenerator idGenerator;

    @Override
    public JobConfigReference create(JobConfigReference model, Identifier identifier) {
        return super.create(model, identifier);
    }

    @Override
    public Optional<JobConfigReference> createV1(JobConfigReference model, Identifier identifier) {
        try {
            if (model.getId() == null || model.getId().isEmpty()) {
                model.setId(idGenerator.generateId());
            }
            
            jdbcTemplate.update(getSql("JobConfigReference.create"),
                    model.getId(),
                    model.getItemId(),
                    model.getJobId(),
                    model.getCreatedBy(),
                    DateUtils.getUnixTimestampInUTC());
            
            return getV1(new Identifier(model.getId()));
        } catch (Exception e) {
            log.error("Error creating JobConfigReference", e);
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, e.getMessage());
        }
    }

    @Override
    public Optional<JobConfigReference> getV1(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("JobConfigReference.getById"),
                    jobConfigReferenceRowMapper,
                    identifier.getWord()
            ));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    @Override
    public JobConfigReference get(Identifier identifier, String whereClause) {
        return super.get(identifier, whereClause);
    }

    @Override
    public Optional<JobConfigReference> getV1(Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public List<JobConfigReference> list(Identifier identifier) {
        return List.of();
    }

    @Override
    public List<JobConfigReference> list(Identifier identifier, String whereClause) {
        return List.of();
    }

    @Override
    public int count(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public JobConfigReference update(JobConfigReference transientObject, Identifier identifier) {
        return super.update(transientObject, identifier);
    }

    @Override
    public Optional<JobConfigReference> updateV1(JobConfigReference transientObject, Identifier identifier) {
        return Optional.empty();
    }

    @Override
    public Optional<JobConfigReference> hotUpdate(JobConfigReference transientObject, Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public int delete(JobConfigReference persistentObject) {
        return 0;
    }

    @Override
    public int deleteV1(Optional<JobConfigReference> persistentObject) {
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
    public <E extends Number> String setInvalues(String query, String replaceString, Set<E> inValues, String... delimitter) {
        return super.setInvalues(query, replaceString, inValues, delimitter);
    }

    @Override
    public String setInvalues(String query, String replaceString, Set<String> inValues) {
        return super.setInvalues(query, replaceString, inValues);
    }

    public List<JobConfigReference> getByItemId(Identifier identifier) {
        try {
            return jdbcTemplate.query(
                    getSql("JobConfigReference.getByItemId"),
                    jobConfigReferenceRowMapper,
                    identifier.getWord()
            );
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public Optional<JobConfigReference> getByJobId(String jobId) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("JobConfigReference.getByJobId"),
                    jobConfigReferenceRowMapper,
                    jobId
            ));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public int deleteById(String id) {
        try {
            return jdbcTemplate.update(getSql("JobConfigReference.deleteById"),
                         "admin",
                        DateUtils.getUnixTimestampInUTC(),
                        DateUtils.getUnixTimestampInUTC(),
                        id);
        } catch (Exception e) {
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR, e.getMessage());
        }
    }

    RowMapper<JobConfigReference> jobConfigReferenceRowMapper = (rs, rowNum) -> {
        JobConfigReference reference = new JobConfigReference();
        reference.setId(rs.getString("id"));
        reference.setItemId(rs.getString("item_id"));
        reference.setJobId(rs.getString("job_id"));
        reference.setCreatedBy(rs.getString("created_by"));
        reference.setCreatedAt(rs.getLong("created_at"));
        return reference;
    };

    @Override
    public Long insert(JobConfigReference model, Identifier identifier) {
        return 0L;
    }

    @Override
    public JobConfigReference upsert(JobConfigReference model, Identifier identifier) {
        return super.upsert(model, identifier);
    }

    @Override
    public JobConfigReference upsert(JobConfigReference model, Identifier identifier, String whereClause) {
        return super.upsert(model, identifier, whereClause);
    }

    @Override
    public JobConfigReference get(Identifier identifier) {
        return super.get(identifier);
    }
}