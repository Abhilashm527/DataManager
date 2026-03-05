package com.dataflow.dataloaders.services.dag;

import com.dataflow.dataloaders.dao.dag.EdgeDao;
import com.dataflow.dataloaders.entity.dagmodels.dag.Edge;
import com.dataflow.dataloaders.util.Identifier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class EdgeService {

    @Autowired
    private EdgeDao edgeDao;

    public Optional<Edge> createEdge(Edge edge) {
        return edgeDao.createV1(edge, Identifier.builder().build());
    }

    public Optional<Edge> getEdgeById(String edgeId) {
        return edgeDao.getV1(Identifier.builder().word(edgeId).build());
    }

    public List<Edge> getEdgesByDataflowId(String dataflowId) {
        return edgeDao.getByDataflowId(dataflowId);
    }

    public int updateEdge(Edge edge) {
        return edgeDao.update(edge);
    }

    public int deleteEdge(String edgeId) {
        Edge edge = new Edge();
        edge.setEdgeId(edgeId);
        return edgeDao.delete(edge);
    }
}
