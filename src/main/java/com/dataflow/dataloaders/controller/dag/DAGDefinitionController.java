package com.dataflow.dataloaders.controller.dag;

import com.dataflow.dataloaders.dto.DAGExecutionResponse;
import com.dataflow.dataloaders.entity.dagmodels.dag.DAGDefinition;
import com.dataflow.dataloaders.services.dag.DAGDefinitionService;
import com.dataflow.dataloaders.util.Response;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

import static com.dataflow.dataloaders.config.APIConstants.DAG_DEFINITIONS_BASE_PATH;

@Slf4j
@RestController
@RequestMapping(DAG_DEFINITIONS_BASE_PATH)
@Tag(name = "DAG Definitions", description = "DAG Definition management APIs")
public class DAGDefinitionController {

    @Autowired
    private DAGDefinitionService dagDefinitionService;

    @Operation(summary = "Save full DAG definition (including nodes and edges)")
    @PostMapping
    public ResponseEntity<Response> create(@RequestBody DAGDefinition dag) {
        log.info("Saving full DAG definition: {}", dag.getDagName());
        return Response.createResponse(dagDefinitionService.saveFullDAG(dag).orElse(null));
    }

    @Operation(summary = "Get DAG definition by ID")
    @GetMapping("/{dagId}")
    public ResponseEntity<Response> get(@Parameter(description = "DAG ID") @PathVariable String dagId) {
        log.info("Getting DAG: {}", dagId);
        return Response.getResponse(dagDefinitionService.getDAGById(dagId).orElse(null));
    }

    @Operation(summary = "Get DAG definitions by dataflow ID")
    @GetMapping("/dataflow/{dataflowId}")
    public ResponseEntity<Response> getByDataflowId(
            @Parameter(description = "Dataflow ID") @PathVariable String dataflowId) {
        log.info("Getting DAG definitions for dataflow: {}", dataflowId);
        List<DAGDefinition> dags = dagDefinitionService.getDAGsByDataflowId(dataflowId);
        return Response.listResponse(dags);
    }

    @Operation(summary = "Update full DAG definition (including nodes and edges)")
    @PutMapping("/{dagId}")
    public ResponseEntity<Response> update(@Parameter(description = "DAG ID") @PathVariable String dagId,
            @RequestBody DAGDefinition dag) {
        log.info("Updating full DAG: {}", dagId);
        dag.setDagId(dagId);
        return Response.updateResponse(dagDefinitionService.saveFullDAG(dag).orElse(null));
    }

    @Operation(summary = "Delete DAG definition")
    @DeleteMapping("/{dagId}")
    public ResponseEntity<Response> delete(@Parameter(description = "DAG ID") @PathVariable String dagId) {
        log.info("Deleting DAG: {}", dagId);
        return Response.deleteResponse(dagDefinitionService.deleteDAG(dagId));
    }

    @Operation(summary = "Execute/Prepare DAG definition")
    @PostMapping("/execute/{dataflowId}")
    public ResponseEntity<Response> execute(
            @PathVariable String dataflowId,
            @RequestHeader(HttpHeaders.AUTHORIZATION) String authHeader) {
        log.info("Executing/Preparing DAG definition for dataflow: {}", dataflowId);
        DAGExecutionResponse result = dagDefinitionService.executeDAG(dataflowId, authHeader);
        return Response.createResponse(result);
    }
}
