package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.entity.ConnectionType;
import com.dataflow.dataloaders.services.ConnectionTypeService;
import com.dataflow.dataloaders.util.Identifier;
import com.dataflow.dataloaders.util.Response;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import static com.dataflow.dataloaders.config.APIConstants.CONNECTION_TYPES_BASE_PATH;

@Slf4j
@RestController
@RequestMapping(CONNECTION_TYPES_BASE_PATH)
@Tag(name = "Connection Types", description = "Connection type management APIs")
public class ConnectionTypeController {

    @Autowired
    private ConnectionTypeService connectionTypeService;

    @Operation(summary = "Create connection type with optional icon upload")
    @PostMapping()
    public ResponseEntity<Response> create(
            @RequestBody ConnectionType connectionType,
            @RequestHeader HttpHeaders headers) {
        log.info("Creating connection type: {}", connectionType.getConnectionType());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.createResponse(connectionTypeService.create(connectionType, identifier));
    }

    @Operation(summary = "Get connection type by ID")
    @GetMapping("/{typeId}")
    public ResponseEntity<Response> get(@Parameter(description = "Connection Type ID") @PathVariable String typeId,
            @RequestHeader HttpHeaders headers) {
        log.info("Getting connection type: {}", typeId);
        Identifier identifier = Identifier.builder().headers(headers).word(typeId).build();
        return Response.getResponse(connectionTypeService.getConnectionType(identifier));
    }

    @Operation(summary = "Get all connection types")
    @GetMapping
    public ResponseEntity<Response> getAll(@RequestHeader HttpHeaders headers) {
        log.info("Getting all connection types");
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(connectionTypeService.getAllConnectionTypes(identifier));
    }

    @Operation(summary = "Update connection type")
    @PutMapping("/{typeId}")
    public ResponseEntity<Response> update(@Parameter(description = "Connection Type ID") @PathVariable String typeId,
            @RequestBody ConnectionType connectionType,
            @RequestHeader HttpHeaders headers) {
        log.info("Updating connection type: {}", typeId);
        Identifier identifier = Identifier.builder().headers(headers).word(typeId).build();
        return Response.updateResponse(connectionTypeService.updateConnectionType(connectionType, identifier));
    }

    @Operation(summary = "Delete connection type")
    @DeleteMapping("/{typeId}")
    public ResponseEntity<Response> delete(@Parameter(description = "Connection Type ID") @PathVariable String typeId,
            @RequestHeader HttpHeaders headers) {
        log.info("Deleting connection type: {}", typeId);
        Identifier identifier = Identifier.builder().headers(headers).word(typeId).build();
        return Response.deleteResponse(connectionTypeService.deleteConnectionType(identifier));
    }
}
