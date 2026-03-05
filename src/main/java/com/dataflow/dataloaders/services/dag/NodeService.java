package com.dataflow.dataloaders.services.dag;

import com.dataflow.dataloaders.dao.dag.NodeDao;
import com.dataflow.dataloaders.entity.dagmodels.dag.Node;
import com.dataflow.dataloaders.util.Identifier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class NodeService {

    @Autowired
    private NodeDao nodeDao;

    public Optional<Node> createNode(Node node) {
        return nodeDao.createV1(node, Identifier.builder().build());
    }

    public Optional<Node> getNodeById(String nodeId) {
        return nodeDao.getV1(Identifier.builder().word(nodeId).build());
    }

    public List<Node> getNodesByDataflowId(String dataflowId) {
        return nodeDao.getByDataflowId(dataflowId);
    }

    public int updateNode(Node node) {
        return nodeDao.update(node);
    }

    public int deleteNode(String nodeId) {
        Node node = new Node();
        node.setNodeId(nodeId);
        return nodeDao.delete(node);
    }
}
