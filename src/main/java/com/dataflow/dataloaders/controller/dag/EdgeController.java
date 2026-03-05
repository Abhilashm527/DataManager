package com.dataflow.dataloaders.controller.dag;

import com.dataflow.dataloaders.entity.dagmodels.dag.Edge;
import com.dataflow.dataloaders.services.dag.EdgeService;
import com.dataflow.dataloaders.util.Response;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

import static com.dataflow.dataloaders.config.APIConstants.DAG_EDGES_BASE_PATH;

@Slf4j
@RestController
@RequestMapping(DAG_EDGES_BASE_PATH)
@Tag(name = "DAG Edges", description = "DAG Edge management APIs")
public class EdgeController {

    @Autowired
    private EdgeService edgeService;

    @Operation(summary = "Create edge")
    @PostMapping
    public ResponseEntity<Response> create(@RequestBody Edge edge) {
        log.info("Creating edge from {} to {}", edge.getSourceNodeId(), edge.getTargetNodeId());
        return Response.createResponse(edgeService.createEdge(edge));
    }

    @Operation(summary = "Get edge by ID")
    @GetMapping("/{edgeId}")
    public ResponseEntity<Response> get(@Parameter(description = "Edge ID") @PathVariable String edgeId) {
        log.info("Getting edge: {}", edgeId);
        return Response.getResponse(edgeService.getEdgeById(edgeId));
    }

    @Operation(summary = "Get edges by dataflow ID")
    @GetMapping("/dataflow/{dataflowId}")
    public ResponseEntity<Response> getByDataflowId(
            @Parameter(description = "Dataflow ID") @PathVariable String dataflowId) {
        log.info("Getting edges for dataflow: {}", dataflowId);
        List<Edge> edges = edgeService.getEdgesByDataflowId(dataflowId);
        return Response.listResponse(edges);
    }

    @Operation(summary = "Update edge")
    @PutMapping("/{edgeId}")
    public ResponseEntity<Response> update(@Parameter(description = "Edge ID") @PathVariable String edgeId,
            @RequestBody Edge edge) {
        log.info("Updating edge: {}", edgeId);
        edge.setEdgeId(edgeId);
        return Response.updateResponse(edgeService.updateEdge(edge));
    }

    @Operation(summary = "Delete edge")
    @DeleteMapping("/{edgeId}")
    public ResponseEntity<Response> delete(@Parameter(description = "Edge ID") @PathVariable String edgeId) {
        log.info("Deleting edge: {}", edgeId);
        return Response.deleteResponse(edgeService.deleteEdge(edgeId));
    }
}
