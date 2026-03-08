package com.dataflow.dataloaders.dao.dag;

import com.dataflow.dataloaders.dao.GenericDaoImpl;
import com.dataflow.dataloaders.entity.dagmodels.dag.Edge;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.DFUtil;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.IdGenerator;
import com.dataflow.dataloaders.util.Identifier;
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
public class EdgeDao extends GenericDaoImpl<Edge, Identifier, String> {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @Autowired
    private DFUtil dfUtil;

    @Autowired
    private IdGenerator idGenerator;

    @Override
    public Optional<Edge> createV1(Edge model, Identifier identifier) {
        try {
            if (model.getEdgeId() == null || model.getEdgeId().isEmpty()) {
                model.setEdgeId(idGenerator.generateId());
            }
            insertEdge(model);
            return getV1(Identifier.builder().word(model.getEdgeId()).build());
        } catch (Exception e) {
            log.error("Error creating edge: {}", e.getMessage());
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, e.getMessage());
        }
    }

    public void insertEdge(Edge model) {
        jdbcTemplate.update(con -> {
            PreparedStatement ps = con.prepareStatement(getSql("Edge.create"));
            ps.setString(1, model.getEdgeId());
            ps.setString(2, model.getDataflowId());
            ps.setString(3, model.getSourceNodeId());
            ps.setString(4, model.getTargetNodeId());
            ps.setString(5, model.getSourcePort());
            ps.setString(6, model.getTargetPort());
            ps.setString(7, model.getEdgeType() != null ? model.getEdgeType().name() : null);
            ps.setString(8, dfUtil.writeValueAsString(model.getCondition()));
            ps.setObject(9, model.getAsync());
            ps.setObject(10, model.getBufferSize());
            ps.setString(11, dfUtil.writeValueAsString(model.getTransformation()));
            ps.setString(12, dfUtil.writeValueAsString(model.getFlowControl()));
            ps.setString(13, dfUtil.writeValueAsString(model.getLineage()));
            ps.setString(14, model.getCreatedBy() != null ? model.getCreatedBy() : "admin");
            ps.setLong(15, DateUtils.getUnixTimestampInUTC());
            return ps;
        });
    }

    @Override
    public Optional<Edge> getV1(Identifier identifier) {
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(
                    getSql("Edge.getById"), edgeRowMapper, identifier.getWord()));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    public List<Edge> getByDataflowId(String dataflowId) {
        try {
            return jdbcTemplate.query(getSql("Edge.getByDataflowId"), edgeRowMapper, dataflowId);
        } catch (EmptyResultDataAccessException e) {
            return List.of();
        }
    }

    public int update(Edge edge) {
        try {
            return jdbcTemplate.update(getSql("Edge.updateById"),
                    edge.getSourceNodeId(),
                    edge.getTargetNodeId(),
                    edge.getSourcePort(),
                    edge.getTargetPort(),
                    edge.getEdgeType() != null ? edge.getEdgeType().name() : null,
                    dfUtil.writeValueAsString(edge.getCondition()),
                    edge.getAsync(),
                    edge.getBufferSize(),
                    dfUtil.writeValueAsString(edge.getTransformation()),
                    dfUtil.writeValueAsString(edge.getFlowControl()),
                    dfUtil.writeValueAsString(edge.getLineage()),
                    edge.getUpdatedBy() != null ? edge.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    edge.getEdgeId());
        } catch (Exception e) {
            handleDatabaseException(e);
            return 0;
        }
    }

    @Override
    public int delete(Edge edge) {
        try {
            return jdbcTemplate.update(getSql("Edge.deleteById"),
                    edge.getUpdatedBy() != null ? edge.getUpdatedBy() : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    DateUtils.getUnixTimestampInUTC(),
                    edge.getEdgeId());
        } catch (Exception e) {
            handleDatabaseException(e);
            return 0;
        }
    }

    public int deleteByNodeId(String nodeId, String updatedBy) {
        try {
            return jdbcTemplate.update(getSql("Edge.deleteByNodeId"),
                    updatedBy != null ? updatedBy : "admin",
                    DateUtils.getUnixTimestampInUTC(),
                    DateUtils.getUnixTimestampInUTC(),
                    nodeId,
                    nodeId);
        } catch (Exception e) {
            handleDatabaseException(e);
            return 0;
        }
    }

    private final RowMapper<Edge> edgeRowMapper = (rs, rowNum) -> {
        Edge edge = new Edge();
        edge.setEdgeId(rs.getString("edge_id"));
        edge.setDataflowId(rs.getString("dataflow_id"));
        edge.setSourceNodeId(rs.getString("source_node_id"));
        edge.setTargetNodeId(rs.getString("target_node_id"));
        edge.setSourcePort(rs.getString("source_port"));
        edge.setTargetPort(rs.getString("target_port"));
        String typeStr = rs.getString("edge_type");
        if (typeStr != null)
            edge.setEdgeType(Edge.EdgeType.valueOf(typeStr));
        edge.setCondition(dfUtil.readValue(Edge.EdgeCondition.class, rs.getString("condition")));
        edge.setAsync(rs.getObject("async") != null ? rs.getBoolean("async") : null);
        edge.setBufferSize(rs.getObject("buffer_size") != null ? rs.getInt("buffer_size") : null);
        edge.setTransformation(dfUtil.readValue(Edge.EdgeTransformation.class, rs.getString("transformation")));
        edge.setFlowControl(dfUtil.readValue(Edge.FlowControl.class, rs.getString("flow_control")));
        edge.setLineage(dfUtil.readValue(Edge.Lineage.class, rs.getString("lineage")));
        return edge;
    };

    @Override
    public Long insert(Edge model, Identifier identifier) {
        return 0L;
    }

    @Override
    public List<Edge> list(Identifier identifier) {
        return List.of();
    }

    @Override
    public Optional<Edge> getV1(Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public List<Edge> list(Identifier identifier, String whereClause) {
        return List.of();
    }

    @Override
    public int count(Identifier identifier, String whereClause) {
        return 0;
    }

    @Override
    public Optional<Edge> updateV1(Edge transientObject, Identifier identifier) {
        return Optional.empty();
    }

    @Override
    public Optional<Edge> hotUpdate(Edge transientObject, Identifier identifier, String whereClause) {
        return Optional.empty();
    }

    @Override
    public int delete(Identifier identifier, String whereClause) {
        return 0;
    }
}
