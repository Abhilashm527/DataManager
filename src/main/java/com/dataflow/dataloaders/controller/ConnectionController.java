package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.dto.TestConnectionRequest;
import com.dataflow.dataloaders.entity.Connection;
import com.dataflow.dataloaders.services.ConnectionService;
import com.dataflow.dataloaders.services.ConnectionTestService;
import com.dataflow.dataloaders.util.Identifier;
import com.dataflow.dataloaders.util.Response;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import static com.dataflow.dataloaders.config.APIConstants.CONNECTIONS_BASE_PATH;

@Slf4j
@RestController
@RequestMapping(CONNECTIONS_BASE_PATH)
@Tag(name = "Connections", description = "Connection management APIs")
public class ConnectionController {

    @Autowired
    private ConnectionService connectionService;

    @Autowired
    private ConnectionTestService connectionTestService;

    @Operation(summary = "Create connection")
    @PostMapping
    public ResponseEntity<Response> create(@RequestBody Connection connection, @RequestHeader HttpHeaders headers) {
        log.info("Creating connection");
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.createResponse(connectionService.create(connection, identifier));
    }

    @Operation(summary = "Test new connection before saving")
    @PostMapping("/test")
    public ResponseEntity<Response> testNewConnection(@RequestBody TestConnectionRequest request, @RequestHeader HttpHeaders headers) {
        log.info("Testing new connection");
        return Response.getResponse(connectionTestService.testNewConnection(request));
    }

    @Operation(summary = "Test existing connection")
    @PostMapping("/{connectionId}/test")
    public ResponseEntity<Response> testExistingConnection(@Parameter(description = "Connection ID") @PathVariable String connectionId,
                                                           @RequestHeader HttpHeaders headers) {
        log.info("Testing existing connection: {}", connectionId);
        Identifier identifier = Identifier.builder().headers(headers).word(connectionId).build();
        return Response.getResponse(connectionTestService.testExistingConnection(connectionId, identifier));
    }

    @Operation(summary = "Get connection by ID")
    @GetMapping("/{connectionId}")
    public ResponseEntity<Response> get(@Parameter(description = "Connection ID") @PathVariable String connectionId,
                                        @RequestHeader HttpHeaders headers) {
        log.info("Getting connection: {}", connectionId);
        Identifier identifier = Identifier.builder().headers(headers).word(connectionId).build();
        return Response.getResponse(connectionService.getConnection(identifier));
    }

    @Operation(summary = "Get all connections")
    @GetMapping
    public ResponseEntity<Response> getAll(@RequestHeader HttpHeaders headers) {
        log.info("Getting all connections");
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(connectionService.getAllConnections(identifier));
    }

    @Operation(summary = "Get connections by application ID")
    @GetMapping("/application/{applicationId}")
    public ResponseEntity<Response> getByApplicationId(@Parameter(description = "Application ID") @PathVariable String applicationId,
                                                @RequestHeader HttpHeaders headers) {
        log.info("Getting connections by application: {}", applicationId);
        return Response.getResponse(connectionService.getConnectionsByApplicationId(applicationId));
    }

    @Operation(summary = "Get connections by provider")
    @GetMapping("/provider/{providerId}")
    public ResponseEntity<Response> getByProvider(@Parameter(description = "Provider ID") @PathVariable String providerId,
                                                  @RequestHeader HttpHeaders headers) {
        log.info("Getting connections by provider: {}", providerId);
        return Response.getResponse(connectionService.getConnectionsByProvider(providerId));
    }

    @Operation(summary = "Update connection")
    @PutMapping("/{connectionId}")
    public ResponseEntity<Response> update(@Parameter(description = "Connection ID") @PathVariable String connectionId,
                                           @RequestBody Connection connection,
                                           @RequestHeader HttpHeaders headers) {
        log.info("Updating connection: {}", connectionId);
        Identifier identifier = Identifier.builder().headers(headers).word(connectionId).build();
        return Response.updateResponse(connectionService.updateConnection(connection, identifier));
    }

    @Operation(summary = "Delete connection")
    @DeleteMapping("/{connectionId}")
    public ResponseEntity<Response> delete(@Parameter(description = "Connection ID") @PathVariable String connectionId,
                                           @RequestHeader HttpHeaders headers) {
        log.info("Deleting connection: {}", connectionId);
        Identifier identifier = Identifier.builder().headers(headers).word(connectionId).build();
        return Response.deleteResponse(connectionService.deleteConnection(identifier));
    }
}
