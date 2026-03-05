package com.dataflow.dataloaders.dao.dag;

import com.dataflow.dataloaders.dao.GenericDaoImpl;
import com.dataflow.dataloaders.entity.dagmodels.dag.Node;
import com.dataflow.dataloaders.entity.JobConfig;
import com.dataflow.dataloaders.entity.dagmodels.dag.Schema;
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
public class NodeDao extends GenericDaoImpl<Node, Identifier, String> {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    private DFUtil dfUtil;

    @Autowired
    private IdGenerator idGenerator;

    @Override
    public Optional<Node> createV1(Node model, Identifier identifier) {
        try {
            if (model.getNodeId() == null || model.getNodeId().isEmpty()) {
                model.setNodeId(idGenerator.generateId());
            }
            insertNode(model);
            return getV1(Identifier.builder().word(model.getNodeId()).build());
        } catch (Exception e) {
            log.error("Error creating node: {}", e.getMessage());
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, e.getMessage());
        }
    }

    public void insertNode(Node model) {
        jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(getSql("Node.create"));
            ps.setString(1, model.getNodeId());
            ps.setString(2, model.getDataflowId());
            ps.setString(3, model.getNodeName());
            ps.setString(4, model.getNodeType() != null ? model.getNodeType().name() : null);
            ps.setString(5, model.getComponentRef());
            ps.setString(6, model.getDescription());
            ps.setString(7, dfUtil.writeValueAsString(model.getPosition()));
            ps.setString(8, dfUtil.writeValueAsString(model.getNodeSchema()));
            ps.setString(9, dfUtil.writeValueAsString(model.getConfig()));
            ps.setString(10, dfUtil.writeValueAsString(model.getInputPorts()));
            ps.setString(11, dfUtil.writeValueAsString(model.getOutputPorts()));
            ps.setString(12, model.getStage());
            ps.setObject(13, model.getStageOrder());
            ps.setString(14, dfUtil.writeValueAsString(model.getDependsOn()));
            ps.setString(15, model.getExecutionMode() != null ? model.getExecutionMode().name() : null);
            ps.setString(16, dfUtil.writeValueAsString(model.getResources()));
            ps.setString(17, dfUtil.writeValueAsString(model.getErrorHandling()));
            ps.setString(18, dfUtil.writeValueAsString(model.getRetryPolicy()));
            ps.setString(19, dfUtil.writeValueAsString(model.getValidations()));
            ps.setString(20, dfUtil.writeValueAsString(model.getCheckpoint()));
            ps.setString(21, dfUtil.writeValueAsString(model.getMapperConfig()));
            ps.setString(22, dfUtil.writeValueAsString(model.getTransformationMetadata()));
            ps.setString(23, model.getCreatedBy() != null ? model.getCreatedBy() : "admin");
            ps.setLong(24, DateUtils.getUnixTimestampInUTC());
            return ps;
        });
    }

    @Override
    public Optional<Node> getV1(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("Node.getById"), nodeRowMapper, identifier.getWord()));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public List<Node> getByDataflowId(String dataflowId) {
        try {
            return jdbcTemplate.query(getSql("Node.getByDataflowId"), nodeRowMapper, dataflowId);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public int update(Node node) {
        try {
            return jdbcTemplate.update(getSql("Node.updateById"),
                    node.getNodeName(),
                    node.getNodeType() != null ? node.getNodeType().name() : null,
                    node.getComponentRef(),
                    node.getDescription(),
                    dfUtil.writeValueAsString(node.getPosition()),
                    dfUtil.writeValueAsString(node.getNodeSchema()),
                    dfUtil.writeValueAsString(node.getConfig()),
                    dfUtil.writeValueAsString(node.getInputPorts()),
                    dfUtil.writeValueAsString(node.getOutputPorts()),
                    node.getStage(),
                    node.getStageOrder(),
                    dfUtil.writeValueAsString(node.getDependsOn()),
                    node.getExecutionMode() != null ? node.getExecutionMode().name() : null,
                    dfUtil.writeValueAsString(node.getResources()),
                    dfUtil.writeValueAsString(node.getErrorHandling()),
                    dfUtil.writeValueAsString(node.getRetryPolicy()),
                    dfUtil.writeValueAsString(node.getValidations()),
                    dfUtil.writeValueAsString(node.getCheckpoint()),
                    dfUtil.writeValueAsString(node.getMapperConfig()),
                    dfUtil.writeValueAsString(node.getTransformationMetadata()),
                    node.getUpdatedBy() != null ? node.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    node.getNodeId());
        } catch (Exception e) {
            handleDatabaseException(e);
            return 0;
        }
    }

    @Override
    public int delete(Node node) {
        try {
            return jdbcTemplate.update(getSql("Node.deleteById"),
                    node.getUpdatedBy() != null ? node.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    DateUtils.getUnixTimestampInUTC(),
                    node.getNodeId());
        } catch (Exception e) {
            handleDatabaseException(e);
            return 0;
        }
    }

    private final RowMapper<Node> nodeRowMapper = (rs, rowNum) -> {
        Node node = new Node();
        node.setNodeId(rs.getString("node_id"));
        node.setDataflowId(rs.getString("dataflow_id"));
        node.setNodeName(rs.getString("node_name"));
        String typeStr = rs.getString("node_type");
        if (typeStr != null)
            node.setNodeType(Node.NodeType.valueOf(typeStr));
        node.setComponentRef(rs.getString("component_ref"));
        node.setDescription(rs.getString("description"));
        node.setPosition(dfUtil.readValue(Node.Position.class, rs.getString("position")));
        node.setNodeSchema(dfUtil.readValue(Schema.class, rs.getString("node_schema")));
        node.setConfig(dfUtil.readValue(JobConfig.class, rs.getString("config")));
        node.setInputPorts(dfUtil.readValue(new TypeReference<List<Node.Port>>() {
        }, rs.getString("input_ports")));
        node.setOutputPorts(dfUtil.readValue(new TypeReference<List<Node.Port>>() {
        }, rs.getString("output_ports")));
        node.setStage(rs.getString("stage"));
        node.setStageOrder(rs.getInt("stage_order"));
        node.setDependsOn(dfUtil.readValue(new TypeReference<List<String>>() {
        }, rs.getString("depends_on")));
        String execModeStr = rs.getString("execution_mode");
        if (execModeStr != null)
            node.setExecutionMode(Node.ExecutionMode.valueOf(execModeStr));
        node.setResources(dfUtil.readValue(Node.Resources.class, rs.getString("resources")));
        node.setErrorHandling(dfUtil.readValue(Node.NodeErrorHandling.class, rs.getString("error_handling")));
        node.setRetryPolicy(dfUtil.readValue(Node.RetryPolicy.class, rs.getString("retry_policy")));
        node.setValidations(dfUtil.readValue(new TypeReference<List<Node.Validation>>() {
        }, rs.getString("validations")));
        node.setCheckpoint(dfUtil.readValue(Node.Checkpoint.class, rs.getString("checkpoint")));
        node.setMapperConfig(dfUtil.readValue(Node.FieldMapperConfig.class, rs.getString("mapper_config")));
        node.setTransformationMetadata(
                dfUtil.readValue(Node.TransformationMetadata.class, rs.getString("transformation_metadata")));
        return node;
    };

    @Override
    public Long insert(Node model, Identifier identifier) {
        return 0L;
    }

    @Override
    public List<Node> list(Identifier identifier) {
        return List.of();
    }

    @Override
    public Optional<Node> getV1(Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public List<Node> list(Identifier identifier, String whereClause) {
        return List.of();
    }

    @Override
    public int count(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public Optional<Node> updateV1(Node transientObject, Identifier identifier) {
        return Optional.empty();
    }

    @Override
    public Optional<Node> hotUpdate(Node transientObject, Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public int delete(Identifier identifier, String whereClause) {
        return 0;
    }
}
