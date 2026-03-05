package com.dataflow.dataloaders.dao.dag;

import com.dataflow.dataloaders.dao.GenericDaoImpl;
import com.dataflow.dataloaders.entity.dagmodels.dag.DAGDefinition;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.DFUtil;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.IdGenerator;
import com.dataflow.dataloaders.util.Identifier;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.Optional;

@Slf4j
@Repository
public class DAGDefinitionDao extends GenericDaoImpl<DAGDefinition, Identifier, String> {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    private DFUtil dfUtil;

    @Autowired
    private IdGenerator idGenerator;

    @Override
    public Optional<DAGDefinition> createV1(DAGDefinition model, Identifier identifier) {
        try {
            if (model.getDagId() == null || model.getDagId().isEmpty()) {
                model.setDagId(idGenerator.generateId());
            }
            insertDAG(model);
            return getV1(Identifier.builder().word(model.getDagId()).build());
        } catch (Exception e) {
            log.error("Error creating DAG: {}", e.getMessage());
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, e.getMessage());
        }
    }

    public void insertDAG(DAGDefinition model) {
        jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(getSql("DAGDefinition.create"));
            ps.setString(1, model.getDagId());
            ps.setString(2, model.getDataflowId());
            ps.setString(3, model.getDagName());
            ps.setString(4, model.getDescription());
            ps.setString(5, model.getType() != null ? model.getType().name() : null);
            ps.setString(6, model.getVersion());
            ps.setString(7, model.getStatus() != null ? model.getStatus().name() : null);
            ps.setString(8, model.getCreatedBy() != null ? model.getCreatedBy() : "admin");
            ps.setLong(9, DateUtils.getUnixTimestampInUTC());
            ps.setObject(10, model.getLastModified() != null ? model.getLastModified().toEpochMilli() : null);
            ps.setString(11, dfUtil.writeValueAsString(model.getTags()));
            ps.setString(12, dfUtil.writeValueAsString(model.getNodeIds()));
            ps.setString(13, dfUtil.writeValueAsString(model.getEdgeIds()));
            ps.setString(14, dfUtil.writeValueAsString(model.getGlobalProperties()));
            ps.setString(15, dfUtil.writeValueAsString(model.getSchedule()));
            ps.setString(16, dfUtil.writeValueAsString(model.getExecutionPlan()));
            ps.setString(17, dfUtil.writeValueAsString(model.getErrorHandling()));
            ps.setString(18, dfUtil.writeValueAsString(model.getMonitoring()));
            ps.setString(19, dfUtil.writeValueAsString(model.getResourceManagement()));
            ps.setString(20, dfUtil.writeValueAsString(model.getMetadata()));
            return ps;
        });
    }

    @Override
    public Optional<DAGDefinition> getV1(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("DAGDefinition.getById"), dagRowMapper, identifier.getWord()));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public List<DAGDefinition> getByDataflowId(String dataflowId) {
        try {
            return jdbcTemplate.query(getSql("DAGDefinition.getByDataflowId"), dagRowMapper, dataflowId);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public int update(DAGDefinition dag) {
        try {
            return jdbcTemplate.update(getSql("DAGDefinition.updateById"),
                    dag.getDagName(),
                    dag.getDescription(),
                    dag.getType() != null ? dag.getType().name() : null,
                    dag.getVersion(),
                    dag.getStatus() != null ? dag.getStatus().name() : null,
                    dag.getLastModified() != null ? dag.getLastModified().toEpochMilli() : null,
                    dfUtil.writeValueAsString(dag.getTags()),
                    dfUtil.writeValueAsString(dag.getNodeIds()),
                    dfUtil.writeValueAsString(dag.getEdgeIds()),
                    dfUtil.writeValueAsString(dag.getGlobalProperties()),
                    dfUtil.writeValueAsString(dag.getSchedule()),
                    dfUtil.writeValueAsString(dag.getExecutionPlan()),
                    dfUtil.writeValueAsString(dag.getErrorHandling()),
                    dfUtil.writeValueAsString(dag.getMonitoring()),
                    dfUtil.writeValueAsString(dag.getResourceManagement()),
                    dfUtil.writeValueAsString(dag.getMetadata()),
                    dag.getUpdatedBy() != null ? dag.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    dag.getDagId());
        } catch (Exception e) {
            handleDatabaseException(e);
            return 0;
        }
    }

    @Override
    public int delete(DAGDefinition dag) {
        try {
            return jdbcTemplate.update(getSql("DAGDefinition.deleteById"),
                    dag.getUpdatedBy() != null ? dag.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    DateUtils.getUnixTimestampInUTC(),
                    dag.getDagId());
        } catch (Exception e) {
            handleDatabaseException(e);
            return 0;
        }
    }

    private final RowMapper<DAGDefinition> dagRowMapper = (rs, rowNum) -> {
        DAGDefinition dag = new DAGDefinition();
        dag.setDagId(rs.getString("dag_id"));
        dag.setDataflowId(rs.getString("dataflow_id"));
        dag.setDagName(rs.getString("dag_name"));
        dag.setDescription(rs.getString("description"));
        String typeStr = rs.getString("dag_type");
        if (typeStr != null)
            dag.setType(DAGDefinition.DAGType.valueOf(typeStr));
        dag.setVersion(rs.getString("version"));
        String statusStr = rs.getString("status");
        if (statusStr != null)
            dag.setStatus(DAGDefinition.DAGStatus.valueOf(statusStr));
        dag.setCreatedBy(rs.getString("created_by"));
        long createdLong = rs.getLong("created_at");
        if (!rs.wasNull())
            dag.setCreatedAt(createdLong);
        long lastMod = rs.getLong("last_modified");
        if (!rs.wasNull())
            dag.setLastModified(java.time.Instant.ofEpochMilli(lastMod));
        dag.setTags(dfUtil.readValue(new TypeReference<List<String>>() {
        }, rs.getString("tags")));
        dag.setNodeIds(dfUtil.readValue(new TypeReference<List<String>>() {
        }, rs.getString("node_ids")));
        dag.setEdgeIds(dfUtil.readValue(new TypeReference<List<String>>() {
        }, rs.getString("edge_ids")));
        dag.setGlobalProperties(
                dfUtil.readValue(DAGDefinition.GlobalProperties.class, rs.getString("global_properties")));
        dag.setSchedule(dfUtil.readValue(DAGDefinition.Schedule.class, rs.getString("schedule")));
        dag.setExecutionPlan(dfUtil.readValue(DAGDefinition.ExecutionPlan.class, rs.getString("execution_plan")));
        dag.setErrorHandling(dfUtil.readValue(DAGDefinition.ErrorHandling.class, rs.getString("error_handling")));
        dag.setMonitoring(dfUtil.readValue(DAGDefinition.Monitoring.class, rs.getString("monitoring")));
        dag.setResourceManagement(
                dfUtil.readValue(DAGDefinition.ResourceManagement.class, rs.getString("resource_management")));
        dag.setMetadata(dfUtil.readValue(DAGDefinition.DAGMetadata.class, rs.getString("metadata")));
        return dag;
    };

    @Override
    public Long insert(DAGDefinition model, Identifier identifier) {
        return 0L;
    }

    @Override
    public List<DAGDefinition> list(Identifier identifier) {
        return List.of();
    }

    @Override
    public Optional<DAGDefinition> getV1(Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public List<DAGDefinition> list(Identifier identifier, String whereClause) {
        return List.of();
    }

    @Override
    public int count(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public Optional<DAGDefinition> updateV1(DAGDefinition transientObject, Identifier identifier) {
        return Optional.empty();
    }

    @Override
    public Optional<DAGDefinition> hotUpdate(DAGDefinition transientObject, Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public int delete(Identifier identifier, String whereClause) {
        return 0;
    }
}
