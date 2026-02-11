package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.services.ConnectionActivityLogService;
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

import static com.dataflow.dataloaders.config.APIConstants.CONNECTION_ACTIVITY_LOGS_BASE_PATH;

@Slf4j
@RestController
@RequestMapping(CONNECTION_ACTIVITY_LOGS_BASE_PATH)
@Tag(name = "Connection Activity Logs", description = "Connection activity log management APIs")
public class ConnectionActivityLogController {

    @Autowired
    private ConnectionActivityLogService activityLogService;

    @Operation(summary = "Get activity log by ID")
    @GetMapping("/{activityLogId}")
    public ResponseEntity<Response> get(@Parameter(description = "Activity Log ID") @PathVariable Long activityLogId,
            @RequestHeader HttpHeaders headers) {
        log.info("Getting activity log: {}", activityLogId);
        Identifier identifier = Identifier.builder().headers(headers).id(activityLogId).build();
        return Response.getResponse(activityLogService.getActivityLog(identifier));
    }

    @Operation(summary = "Get all activity logs")
    @GetMapping
    public ResponseEntity<Response> getAll(@RequestHeader HttpHeaders headers) {
        log.info("Getting all activity logs");
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(activityLogService.getAllActivityLogs(identifier));
    }

    @Operation(summary = "Get activity logs by connection ID")
    @GetMapping("/connection/{connectionId}")
    public ResponseEntity<Response> getByConnectionId(
            @Parameter(description = "Connection ID") @PathVariable Long connectionId,
            @RequestHeader HttpHeaders headers) {
        log.info("Getting activity logs by connection: {}", connectionId);
        return Response.getResponse(activityLogService.getActivityLogsByConnectionId(connectionId));
    }

    @Operation(summary = "Get activity logs by type")
    @GetMapping("/connection/{connectionId}/type/{activityType}")
    public ResponseEntity<Response> getByType(@Parameter(description = "Connection ID") @PathVariable Long connectionId,
            @Parameter(description = "Activity Type") @PathVariable String activityType,
            @RequestHeader HttpHeaders headers) {
        log.info("Getting activity logs by type: {} for connection: {}", activityType, connectionId);
        return Response.getResponse(activityLogService.getActivityLogsByType(connectionId, activityType));
    }

    @Operation(summary = "Delete activity log")
    @DeleteMapping("/{activityLogId}")
    public ResponseEntity<Response> delete(@Parameter(description = "Activity Log ID") @PathVariable Long activityLogId,
            @RequestHeader HttpHeaders headers) {
        log.info("Deleting activity log: {}", activityLogId);
        Identifier identifier = Identifier.builder().headers(headers).id(activityLogId).build();
        return Response.deleteResponse(activityLogService.deleteActivityLog(identifier));
    }
}
