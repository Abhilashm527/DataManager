package com.dataflow.dataloaders.services.dag;

import com.dataflow.dataloaders.dao.dag.DAGDefinitionDao;
import com.dataflow.dataloaders.dto.DAGExecutionResponse;
import com.dataflow.dataloaders.entity.Connection;
import com.dataflow.dataloaders.entity.Provider;
import com.dataflow.dataloaders.entity.dagmodels.dag.DAGDefinition;
import com.dataflow.dataloaders.entity.dagmodels.dag.Node;
import com.dataflow.dataloaders.jobconfigs.ConnectionConfig;
import com.dataflow.dataloaders.services.*;
import com.dataflow.dataloaders.util.Identifier;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.util.List;
import java.util.Optional;

@Slf4j
@Service
public class DAGDefinitionService {

    @Autowired
    private DAGDefinitionDao dagDefinitionDao;

    @Autowired
    private NodeService nodeService;

    @Autowired
    private EdgeService edgeService;

    @Autowired
    private ConnectionService connectionService;

    @Autowired
    private EncryptionService encryptionService;

    @Autowired
    private VariableService variableService;

    @Autowired
    private ProviderService providerService;

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
        List<com.dataflow.dataloaders.entity.dagmodels.dag.Node> allNodes = nodeService
                .getNodesByDataflowId(dataflowId);
        List<com.dataflow.dataloaders.entity.dagmodels.dag.Edge> allEdges = edgeService
                .getEdgesByDataflowId(dataflowId);

        if (dags.isEmpty()) {
            if (!allNodes.isEmpty() || !allEdges.isEmpty()) {
                DAGDefinition defaultDag = new DAGDefinition();
                defaultDag.setDagId("default-" + dataflowId);
                defaultDag.setDataflowId(dataflowId);
                defaultDag.setDagName("Default DAG");
                defaultDag.setNodes(allNodes);
                defaultDag.setEdges(allEdges);

                List<String> nodeIds = new java.util.ArrayList<>();
                for (com.dataflow.dataloaders.entity.dagmodels.dag.Node n : allNodes) {
                    nodeIds.add(n.getNodeId());
                }
                defaultDag.setNodeIds(nodeIds);

                List<String> edgeIds = new java.util.ArrayList<>();
                for (com.dataflow.dataloaders.entity.dagmodels.dag.Edge e : allEdges) {
                    edgeIds.add(e.getEdgeId());
                }
                defaultDag.setEdgeIds(edgeIds);

                return java.util.List.of(defaultDag);
            }
            return dags;
        }

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

    /**
     * Prepares the DAG for execution by binding connection details to nodes.
     */
    public DAGExecutionResponse executeDAG(String dataflowId, String authHeader) {
        List<DAGDefinition> dags = getDAGsByDataflowId(dataflowId);
        if (dags == null || dags.isEmpty()) {
            throw new com.dataflow.dataloaders.exception.DataloadersException(
                    com.dataflow.dataloaders.exception.ErrorFactory.RESOURCE_NOT_FOUND,
                    "No DAG found for dataflowId: " + dataflowId);
        }
        DAGDefinition dag = dags.get(0);
        log.info("Preparing DAG for execution: {}", dag.getDagId());

        if (dag.getNodes() != null) {
            for (Node node : dag.getNodes()) {
                bindConnectionToNode(node);
            }
        }

        // Set execution metadata
        dag.setStatus(DAGDefinition.DAGStatus.ACTIVE);

        // Note: The entities (DAGDefinition, Node, Edge, JobConfig) now use
        // @JsonInclude(JsonInclude.Include.NON_NULL) to ensure null values
        // are omitted during serialization for the external call.

        // Make external call to DAG engine here
        try {
            RestClient restClient = RestClient.create();
            log.info("Making external call to DAG engine for dataflow: {}", dataflowId);

            DAGExecutionResponse response = restClient.post()
                    .uri("http://localhost:8888/api/dag/execute")
                    .header(HttpHeaders.AUTHORIZATION, authHeader)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(dag)
                    .retrieve()
                    .body(DAGExecutionResponse.class);

            log.info("External call to DAG engine successful for dataflow: {}", dataflowId);
            return response;
        } catch (Exception e) {
            log.error("Failed to execute DAG via external engine: {}", e.getMessage(), e);
            throw new com.dataflow.dataloaders.exception.DataloadersException(
                    com.dataflow.dataloaders.exception.ErrorFactory.INTERNAL_SERVER_ERROR,
                    "Failed to communicate with DAG engine: " + e.getMessage());
        }
    }

    private void bindConnectionToNode(Node node) {
        if (node.getConfig() == null)
            return;

        // Bind Reader Connection
        if (node.getConfig().getReaderConfig() != null
                && node.getConfig().getReaderConfig().getConnectionId() != null) {
            String connId = node.getConfig().getReaderConfig().getConnectionId();
            node.getConfig().getReaderConfig().setConnectionConfig(fetchConnectionConfig(connId));
        }

        // Bind Writer Connection
        if (node.getConfig().getWriterConfig() != null
                && node.getConfig().getWriterConfig().getConnectionId() != null) {
            String connId = node.getConfig().getWriterConfig().getConnectionId();
            node.getConfig().getWriterConfig().setConnectionConfig(fetchConnectionConfig(connId));
        }
    }

    private ConnectionConfig fetchConnectionConfig(String connectionId) {
        try {
            Connection connection = connectionService
                    .getConnection(Identifier.builder().word(connectionId).build());
            Provider provider = providerService
                    .getProvider(Identifier.builder().word(connection.getProviderId()).build());
            String providerKey = provider.getProviderName().toLowerCase();

            // Decrypt secrets
            JsonNode decryptedSecrets = encryptionService.decrypt(connection.getSecrets());

            // Resolve variables
            JsonNode resolvedConfig = variableService.resolveJsonNode(connection.getConfig(),
                    connection.getApplicationId(), null);
            JsonNode resolvedSecrets = variableService.resolveJsonNode(decryptedSecrets, connection.getApplicationId(),
                    null);

            ConnectionConfig config = new ConnectionConfig();

            if (providerKey.contains("postgres")) {
                config.setJdbcUrl(String.format("jdbc:postgresql://%s:%d/%s",
                        resolvedConfig.path("host").asText(),
                        resolvedConfig.path("port").asInt(),
                        resolvedConfig.path("database_name").asText()));
                config.setJdbcUser(resolvedConfig.path("username").asText());
                config.setJdbcPassword(resolvedSecrets.path("password").asText());
                config.setJdbcDriverName("org.postgresql.Driver");
            } else if (providerKey.contains("mysql") || providerKey.contains("mariadb")) {
                config.setJdbcUrl(String.format("jdbc:mysql://%s:%d/%s?serverTimezone=UTC",
                        resolvedConfig.path("host").asText(),
                        resolvedConfig.path("port").asInt(),
                        resolvedConfig.path("database_name").asText()));
                config.setJdbcUser(resolvedConfig.path("username").asText());
                config.setJdbcPassword(resolvedSecrets.path("password").asText());
                config.setJdbcDriverName("com.mysql.cj.jdbc.Driver");
            } else if (providerKey.contains("oracle")) {
                config.setJdbcUrl(String.format("jdbc:oracle:thin:@%s:%d:%s",
                        resolvedConfig.path("host").asText(),
                        resolvedConfig.path("port").asInt(),
                        resolvedConfig.path("service_name").asText()));
                config.setJdbcUser(resolvedConfig.path("username").asText());
                config.setJdbcPassword(resolvedSecrets.path("password").asText());
                config.setJdbcDriverName("oracle.jdbc.driver.OracleDriver");
            } else if (providerKey.contains("mssql")) {
                config.setJdbcUrl(String.format("jdbc:sqlserver://%s:%d;databaseName=%s",
                        resolvedConfig.path("host").asText(),
                        resolvedConfig.path("port").asInt(),
                        resolvedConfig.path("database_name").asText()));
                config.setJdbcUser(resolvedConfig.path("username").asText());
                config.setJdbcPassword(resolvedSecrets.path("password").asText());
                config.setJdbcDriverName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            }
            // Add other providers as needed...

            return config;
        } catch (Exception e) {
            log.error("Error binding connection {}: {}", connectionId, e.getMessage());
            return null;
        }
    }
}
