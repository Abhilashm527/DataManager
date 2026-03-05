package com.dataflow.dataloaders.controller.dag;

import com.dataflow.dataloaders.entity.dagmodels.dag.Node;
import com.dataflow.dataloaders.services.dag.NodeService;
import com.dataflow.dataloaders.util.Response;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

import static com.dataflow.dataloaders.config.APIConstants.DAG_NODES_BASE_PATH;

@Slf4j
@RestController
@RequestMapping(DAG_NODES_BASE_PATH)
@Tag(name = "DAG Nodes", description = "DAG Node management APIs")
public class NodeController {

    @Autowired
    private NodeService nodeService;

    @Operation(summary = "Create node")
    @PostMapping
    public ResponseEntity<Response> create(@RequestBody Node node) {
        log.info("Creating node: {}", node.getNodeName());
        return Response.createResponse(nodeService.createNode(node).orElse(null));
    }

    @Operation(summary = "Get node by ID")
    @GetMapping("/{nodeId}")
    public ResponseEntity<Response> get(@Parameter(description = "Node ID") @PathVariable String nodeId) {
        log.info("Getting node: {}", nodeId);
        return Response.getResponse(nodeService.getNodeById(nodeId).orElse(null));
    }

    @Operation(summary = "Get nodes by dataflow ID")
    @GetMapping("/dataflow/{dataflowId}")
    public ResponseEntity<Response> getByDataflowId(
            @Parameter(description = "Dataflow ID") @PathVariable String dataflowId) {
        log.info("Getting nodes for dataflow: {}", dataflowId);
        List<Node> nodes = nodeService.getNodesByDataflowId(dataflowId);
        return Response.listResponse(nodes);
    }

    @Operation(summary = "Update node")
    @PutMapping("/{nodeId}")
    public ResponseEntity<Response> update(@Parameter(description = "Node ID") @PathVariable String nodeId,
            @RequestBody Node node) {
        log.info("Updating node: {}", nodeId);
        node.setNodeId(nodeId);
        return Response.updateResponse(nodeService.updateNode(node));
    }

    @Operation(summary = "Delete node")
    @DeleteMapping("/{nodeId}")
    public ResponseEntity<Response> delete(@Parameter(description = "Node ID") @PathVariable String nodeId) {
        log.info("Deleting node: {}", nodeId);
        return Response.deleteResponse(nodeService.deleteNode(nodeId));
    }
}
