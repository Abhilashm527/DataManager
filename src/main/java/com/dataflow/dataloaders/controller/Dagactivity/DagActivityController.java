package com.dataflow.dataloaders.controller.Dagactivity;

import com.dataflow.dataloaders.dto.JdbcDataPreviewResponse;
import com.dataflow.dataloaders.dto.JdbcSchemaTreeResponse;
import com.dataflow.dataloaders.dto.JdbcTableDefinitionResponse;
import com.dataflow.dataloaders.services.DagActivityService;
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

import static com.dataflow.dataloaders.config.APIConstants.DAG_ACTIVITY_PATH;

@Slf4j
@RestController
@RequestMapping(DAG_ACTIVITY_PATH)
@Tag(name = "DAG Activity", description = "Endpoints for live DAG node interactions (JDBC inspection, etc.)")
public class DagActivityController {

    @Autowired
    private DagActivityService dagActivityService;

    @Operation(summary = "Fetch tables for a connection")
    @GetMapping("/jdbc/tables")
    public ResponseEntity<Response> fetchTables(
            @Parameter(description = "ID of the saved connection") @RequestParam String connectionId,
            @RequestHeader HttpHeaders headers) {
        log.info("Request to fetch tables for connection: {}", connectionId);
        List<JdbcSchemaTreeResponse> tables = dagActivityService.fetchTables(connectionId, headers);
        return Response.getResponse(tables);
    }

    @Operation(summary = "Preview data for a table or query")
    @GetMapping("/jdbc/preview")
    public ResponseEntity<Response> previewData(
            @Parameter(description = "ID of the saved connection") @RequestParam String connectionId,
            @Parameter(description = "Schema name") @RequestParam(required = false) String schemaName,
            @Parameter(description = "Table name (optional if query is provided)") @RequestParam(required = false) String tableName,
            @Parameter(description = "Custom SQL query") @RequestParam(required = false) String query,
            @Parameter(description = "Max records to return") @RequestParam(required = false, defaultValue = "50") Integer limit,
            @RequestHeader HttpHeaders headers) {
        log.info("Request to preview data for connection: {}", connectionId);
        JdbcDataPreviewResponse preview = dagActivityService.previewData(connectionId, schemaName, tableName, query,
                limit,
                headers);
        return Response.getResponse(preview);
    }

    @Operation(summary = "Get detailed definition for a table")
    @GetMapping("/jdbc/ddl")
    public ResponseEntity<Response> getTableDefinition(
            @Parameter(description = "ID of the saved connection") @RequestParam String connectionId,
            @Parameter(description = "Schema name") @RequestParam(required = false) String schemaName,
            @Parameter(description = "Table name") @RequestParam String tableName,
            @RequestHeader HttpHeaders headers) {
        log.info("Request to get definition for table: {} in connection: {}", tableName, connectionId);
        JdbcTableDefinitionResponse definition = dagActivityService.getTableDefinition(connectionId, schemaName,
                tableName,
                headers);
        return Response.getResponse(definition);
    }
}
