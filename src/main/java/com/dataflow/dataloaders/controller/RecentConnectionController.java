package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.services.RecentConnectionService;
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

import static com.dataflow.dataloaders.config.APIConstants.RECENT_CONNECTIONS_BASE_PATH;

@Slf4j
@RestController
@RequestMapping(RECENT_CONNECTIONS_BASE_PATH)
@Tag(name = "Recent Connections", description = "Recent connection management APIs")
public class RecentConnectionController {

    @Autowired
    private RecentConnectionService recentConnectionService;

    @Operation(summary = "Get recent connection by ID")
    @GetMapping("/{recentConnectionId}")
    public ResponseEntity<Response> get(
            @Parameter(description = "Recent Connection ID") @PathVariable Long recentConnectionId,
            @RequestHeader HttpHeaders headers) {
        log.info("Getting recent connection: {}", recentConnectionId);
        Identifier identifier = Identifier.builder().headers(headers).id(recentConnectionId).build();
        return Response.getResponse(recentConnectionService.getRecentConnection(identifier));
    }

    @Operation(summary = "Get all recent connections")
    @GetMapping
    public ResponseEntity<Response> getAll(@RequestHeader HttpHeaders headers) {
        log.info("Getting all recent connections");
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(recentConnectionService.getAllRecentConnections(identifier));
    }

    @Operation(summary = "Get recent connections by user ID")
    @GetMapping("/user/{userId}")
    public ResponseEntity<Response> getByUserId(@Parameter(description = "User ID") @PathVariable Long userId,
            @RequestHeader HttpHeaders headers) {
        log.info("Getting recent connections by user: {}", userId);
        return Response.getResponse(recentConnectionService.getRecentConnectionsByUserId(userId));
    }

    @Operation(summary = "Delete a specific recent connection")
    @DeleteMapping("/{recentConnectionId}")
    public ResponseEntity<Response> delete(
            @Parameter(description = "Recent Connection ID") @PathVariable Long recentConnectionId,
            @RequestHeader HttpHeaders headers) {
        log.info("Deleting recent connection: {}", recentConnectionId);
        Identifier identifier = Identifier.builder().headers(headers).id(recentConnectionId).build();
        return Response.deleteResponse(recentConnectionService.deleteRecentConnection(identifier));
    }

    @Operation(summary = "Clear all recent connections for a user")
    @DeleteMapping("/user/{userId}")
    public ResponseEntity<Response> clearAllByUser(@Parameter(description = "User ID") @PathVariable Long userId,
            @RequestHeader HttpHeaders headers) {
        log.info("Clearing all recent connections for user: {}", userId);
        int deletedCount = recentConnectionService.deleteAllByUserId(userId);
        return Response.deleteResponse(deletedCount > 0);
    }

}
