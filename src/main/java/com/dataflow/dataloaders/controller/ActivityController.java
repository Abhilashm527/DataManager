package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.services.ActivityLogService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@Controller
public class ActivityController {

    @Autowired
    private ActivityLogService activityLogService;

    @MessageMapping("/activity")
    @SendTo("/topic/activities")
    public String handleActivity(String message) {
        log.info("Received activity message: {}", message);
        return message;
    }
}
@Slf4j
@RestController
@RequestMapping("/api/v1/activities")
@Tag(name = "Activities", description = "Activity log management APIs")
class ActivityRestController {

    @Autowired
    private ActivityLogService activityLogService;

    @Operation(summary = "Test activity", description = "Test activity logging")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Activity logged successfully")
    })
    @GetMapping("/test")
    public String testActivity() {
        log.info("Test activity endpoint called");
        activityLogService.logActivity("TEST", "SYSTEM", "test-id", "Test Activity", "CREATED", "Test activity log", "admin");
        return "Activity logged and broadcasted via WebSocket";
    }
    
    @Operation(summary = "Get WebSocket info", description = "Get WebSocket connection information")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "WebSocket info retrieved")
    })
    @GetMapping("/websocket-info")
    public String getWebSocketInfo() {
        return "WebSocket endpoint: ws://localhost:8083/ws/activity | Topic: /topic/activities";
    }
    
    @Operation(summary = "Get recent activities", description = "Get recent activity logs")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Recent activities retrieved")
    })
    @GetMapping("/recent")
    public java.util.List<com.dataflow.dataloaders.entity.ActivityLog> getRecentActivities() {
        return activityLogService.getRecentActivities();
    }
    
    @Operation(summary = "Get all activities", description = "Get all activity logs")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "All activities retrieved")
    })
    @GetMapping("/all")
    public java.util.List<com.dataflow.dataloaders.entity.ActivityLog> getAllActivities() {
        return activityLogService.getAllActivities();
    }
}