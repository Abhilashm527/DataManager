package com.dataflow.dataloaders.services.dag;

import com.dataflow.dataloaders.dao.dag.DAGDefinitionDao;
import com.dataflow.dataloaders.entity.dagmodels.dag.DAGDefinition;
import com.dataflow.dataloaders.util.Identifier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class DAGDefinitionService {

    @Autowired
    private DAGDefinitionDao dagDefinitionDao;

    @Autowired
    private NodeService nodeService;

    @Autowired
    private EdgeService edgeService;

    public Optional<DAGDefinition> saveFullDAG(DAGDefinition dag) {
        String dataflowId = dag.getDataflowId();

        // 1. Save/Update Nodes
        java.util.List<String> nodeIds = new java.util.ArrayList<>();
        if (dag.getNodes() != null) {
            for (com.dataflow.dataloaders.entity.dagmodels.dag.Node node : dag.getNodes()) {
                node.setDataflowId(dataflowId);
                if (node.getNodeId() == null || node.getNodeId().isEmpty()) {
                    nodeService.createNode(node).ifPresent(n -> nodeIds.add(n.getNodeId()));
                } else {
                    nodeService.updateNode(node);
                    nodeIds.add(node.getNodeId());
                }
            }
        }
        dag.setNodeIds(nodeIds);

        // 2. Save/Update Edges
        java.util.List<String> edgeIds = new java.util.ArrayList<>();
        if (dag.getEdges() != null) {
            for (com.dataflow.dataloaders.entity.dagmodels.dag.Edge edge : dag.getEdges()) {
                edge.setDataflowId(dataflowId);
                if (edge.getEdgeId() == null || edge.getEdgeId().isEmpty()) {
                    edgeService.createEdge(edge).ifPresent(e -> edgeIds.add(e.getEdgeId()));
                } else {
                    edgeService.updateEdge(edge);
                    edgeIds.add(edge.getEdgeId());
                }
            }
        }
        dag.setEdgeIds(edgeIds);

        // 3. Save DAG Definition
        if (dag.getDagId() == null || dag.getDagId().isEmpty()) {
            return dagDefinitionDao.createV1(dag, Identifier.builder().build());
        } else {
            dagDefinitionDao.update(dag);
            return Optional.of(dag);
        }
    }

    public Optional<DAGDefinition> createDAG(DAGDefinition dag) {
        return dagDefinitionDao.createV1(dag, Identifier.builder().build());
    }

    public Optional<DAGDefinition> getDAGById(String dagId) {
        return dagDefinitionDao.getV1(Identifier.builder().word(dagId).build());
    }

    public List<DAGDefinition> getDAGsByDataflowId(String dataflowId) {
        return dagDefinitionDao.getByDataflowId(dataflowId);
    }

    public int updateDAG(DAGDefinition dag) {
        return dagDefinitionDao.update(dag);
    }

    public int deleteDAG(String dagId) {
        DAGDefinition dag = new DAGDefinition();
        dag.setDagId(dagId);
        return dagDefinitionDao.delete(dag);
    }
}
