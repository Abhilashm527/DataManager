package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.services.ActivityLogService;
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
class ActivityRestController {

    @Autowired
    private ActivityLogService activityLogService;

    @GetMapping("/test")
    public String testActivity() {
        log.info("Test activity endpoint called");
        activityLogService.logActivity("TEST", "SYSTEM", "test-id", "Test Activity", "CREATED", "Test activity log", "admin");
        return "Activity logged and broadcasted via WebSocket";
    }
    
    @GetMapping("/websocket-info")
    public String getWebSocketInfo() {
        return "WebSocket endpoint: ws://localhost:8083/ws/activity | Topic: /topic/activities";
    }
    
    @GetMapping("/recent")
    public java.util.List<com.dataflow.dataloaders.entity.ActivityLog> getRecentActivities() {
        return activityLogService.getRecentActivities();
    }
    
    @GetMapping("/all")
    public java.util.List<com.dataflow.dataloaders.entity.ActivityLog> getAllActivities() {
        return activityLogService.getAllActivities();
    }
}