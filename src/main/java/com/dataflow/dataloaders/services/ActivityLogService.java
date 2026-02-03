package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dao.ActivityLogDao;
import com.dataflow.dataloaders.entity.ActivityLog;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.IdGenerator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ActivityLogService {

    @Autowired
    private IdGenerator idGenerator;

    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    @Autowired
    private ActivityLogDao activityLogDao;

    private final java.util.List<ActivityLog> recentActivities = new java.util.concurrent.CopyOnWriteArrayList<>();

    public void logActivity(String activityType, String entityType, String entityId,
                            String entityName, String action, String description, String userId) {

        ActivityLog activity = ActivityLog.builder()
                .id(idGenerator.generateId())
                .activityType(activityType)
                .entityType(entityType)
                .entityId(entityId)
                .entityName(entityName)
                .action(action)
                .description(description)
                .userId(userId)
                .createdAt(DateUtils.getUnixTimestampInUTC())
                .build();

        // Store in database
        activityLogDao.createV1(activity, new com.dataflow.dataloaders.util.Identifier());
        
        recentActivities.add(0, activity);
        if (recentActivities.size() > 50) {
            recentActivities.remove(recentActivities.size() - 1);
        }

        messagingTemplate.convertAndSend("/topic/activities", activity);

        log.info("Activity logged: {} {} {} by {}", action, entityType, entityName, userId);
    }

    // Convenience methods for common activities
    public void logResourceActivity(String action, String resourceId, String resourceName, String userId) {
        logActivity("RESOURCE", "RESOURCE", resourceId, resourceName, action, 
                   action + " resource: " + resourceName, userId);
    }

    public void logDataflowActivity(String action, String jobId, String jobName, String userId) {
        logActivity("DATAFLOW", "JOB_CONFIG", jobId, jobName, action, 
                   action + " dataflow: " + jobName, userId);
    }

    public void logApplicationActivity(String action, String itemId, String itemName, String userId) {
        logActivity("APPLICATION", "ITEM", itemId, itemName, action, 
                   action + " application: " + itemName, userId);
    }
    
    public java.util.List<ActivityLog> getRecentActivities() {
        return activityLogDao.getRecentActivities(50);
    }
    
    public java.util.List<ActivityLog> getAllActivities() {
        return activityLogDao.list(new com.dataflow.dataloaders.util.Identifier());
    }
}