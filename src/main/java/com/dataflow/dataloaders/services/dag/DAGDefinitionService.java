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
        Optional<DAGDefinition> dagOpt = dagDefinitionDao.getV1(Identifier.builder().word(dagId).build());
        if (dagOpt.isPresent()) {
            DAGDefinition dag = dagOpt.get();
            if (dag.getDataflowId() != null) {
                List<com.dataflow.dataloaders.entity.dagmodels.dag.Node> allNodes = nodeService
                        .getNodesByDataflowId(dag.getDataflowId());
                List<com.dataflow.dataloaders.entity.dagmodels.dag.Edge> allEdges = edgeService
                        .getEdgesByDataflowId(dag.getDataflowId());

                List<com.dataflow.dataloaders.entity.dagmodels.dag.Node> dagNodes = new java.util.ArrayList<>();
                if (dag.getNodeIds() != null && !dag.getNodeIds().isEmpty()) {
                    for (com.dataflow.dataloaders.entity.dagmodels.dag.Node n : allNodes) {
                        if (dag.getNodeIds().contains(n.getNodeId())) {
                            dagNodes.add(n);
                        }
                    }
                } else {
                    dagNodes.addAll(allNodes);
                }
                dag.setNodes(dagNodes);

                List<com.dataflow.dataloaders.entity.dagmodels.dag.Edge> dagEdges = new java.util.ArrayList<>();
                if (dag.getEdgeIds() != null && !dag.getEdgeIds().isEmpty()) {
                    for (com.dataflow.dataloaders.entity.dagmodels.dag.Edge e : allEdges) {
                        if (dag.getEdgeIds().contains(e.getEdgeId())) {
                            dagEdges.add(e);
                        }
                    }
                } else {
                    dagEdges.addAll(allEdges);
                }
                dag.setEdges(dagEdges);
            }
        }
        return dagOpt;
    }

    public List<DAGDefinition> getDAGsByDataflowId(String dataflowId) {
        List<DAGDefinition> dags = dagDefinitionDao.getByDataflowId(dataflowId);
        if (dags.isEmpty()) {
            return dags;
        }

        List<com.dataflow.dataloaders.entity.dagmodels.dag.Node> allNodes = nodeService
                .getNodesByDataflowId(dataflowId);
        List<com.dataflow.dataloaders.entity.dagmodels.dag.Edge> allEdges = edgeService
                .getEdgesByDataflowId(dataflowId);

        for (DAGDefinition dag : dags) {
            List<com.dataflow.dataloaders.entity.dagmodels.dag.Node> dagNodes = new java.util.ArrayList<>();
            if (dag.getNodeIds() != null && !dag.getNodeIds().isEmpty()) {
                for (com.dataflow.dataloaders.entity.dagmodels.dag.Node n : allNodes) {
                    if (dag.getNodeIds().contains(n.getNodeId())) {
                        dagNodes.add(n);
                    }
                }
            } else {
                dagNodes.addAll(allNodes);
            }
            dag.setNodes(dagNodes);

            List<com.dataflow.dataloaders.entity.dagmodels.dag.Edge> dagEdges = new java.util.ArrayList<>();
            if (dag.getEdgeIds() != null && !dag.getEdgeIds().isEmpty()) {
                for (com.dataflow.dataloaders.entity.dagmodels.dag.Edge e : allEdges) {
                    if (dag.getEdgeIds().contains(e.getEdgeId())) {
                        dagEdges.add(e);
                    }
                }
            } else {
                dagEdges.addAll(allEdges);
            }
            dag.setEdges(dagEdges);
        }
        return dags;
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
