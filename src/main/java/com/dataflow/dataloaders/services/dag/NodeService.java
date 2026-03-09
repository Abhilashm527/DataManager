package com.dataflow.dataloaders.services.dag;

import com.dataflow.dataloaders.dao.dag.NodeDao;
import com.dataflow.dataloaders.dao.dag.EdgeDao;
import com.dataflow.dataloaders.entity.ReaderWriterConfigType;
import com.dataflow.dataloaders.entity.dagmodels.dag.Node;
import com.dataflow.dataloaders.services.ReaderWriterConfigService;
import com.dataflow.dataloaders.util.Identifier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Service
public class NodeService {

    @Autowired
    private NodeDao nodeDao;

    @Autowired
    private EdgeDao edgeDao;
    @Autowired
    private ReaderWriterConfigService readerWriterConfigService;

    @Autowired
    private com.dataflow.dataloaders.validator.NodeValidator nodeValidator;

    public Optional<Node> createNode(Node node) {
        nodeValidator.validate(node);
        if (node.getCreatedBy() == null) {
            node.setCreatedBy("admin");
        }
        cleanConfigValues(node);
        return nodeDao.createV1(node, Identifier.builder().build());
    }

    private void cleanConfigValues(Node node) {
        if (node.getConfig() != null) {
            try {
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                if (node.getConfig().getReaderConfig() != null) {
                    java.util.Map<String, Object> readerMap = mapper.convertValue(
                            node.getConfig().getReaderConfig(),
                            new com.fasterxml.jackson.core.type.TypeReference<java.util.Map<String, Object>>() {
                            });
                    readerMap.entrySet().removeIf(
                            entry -> entry.getValue() == null || String.valueOf(entry.getValue()).trim().isEmpty());
                    node.getConfig().setReaderConfig(
                            mapper.convertValue(readerMap, com.dataflow.dataloaders.jobconfigs.ReaderConfig.class));
                }
                if (node.getConfig().getWriterConfig() != null) {
                    java.util.Map<String, Object> writerMap = mapper.convertValue(
                            node.getConfig().getWriterConfig(),
                            new com.fasterxml.jackson.core.type.TypeReference<java.util.Map<String, Object>>() {
                            });
                    writerMap.entrySet().removeIf(
                            entry -> entry.getValue() == null || String.valueOf(entry.getValue()).trim().isEmpty());
                    node.getConfig().setWriterConfig(
                            mapper.convertValue(writerMap, com.dataflow.dataloaders.jobconfigs.WriterConfig.class));
                }
            } catch (Exception e) {
                // Continue if cleaning fails
            }
        }
    }

    public Optional<Node> getNodeById(String nodeId) {
        return nodeDao.getV1(Identifier.builder().word(nodeId).build());
    }

    public List<Node> getNodesByDataflowId(String dataflowId) {
        return nodeDao.getByDataflowId(dataflowId);
    }

    public int updateNode(Node node) {
        nodeValidator.validate(node);
        if (node.getUpdatedBy() == null) {
            node.setUpdatedBy("admin");
        }
        return nodeDao.update(node);
    }

    @Transactional
    public int deleteNode(String nodeId) {
        // Cascade delete associated edges first
        edgeDao.deleteByNodeId(nodeId, "admin");

        Node node = new Node();
        node.setNodeId(nodeId);
        node.setUpdatedBy("admin");
        return nodeDao.delete(node);
    }
}
