package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dao.ConnectionActivityLogDao;
import com.dataflow.dataloaders.entity.ConnectionActivityLog;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.Identifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.List;

@Slf4j
@Service
public class ConnectionActivityLogService {

    @Autowired
    private ConnectionActivityLogDao activityLogDao;

    public ConnectionActivityLog create(ConnectionActivityLog activityLog, Identifier identifier) {
        log.info("Creating connection activity log");
        return activityLogDao.create(activityLog, identifier);
    }


    public void logActivity(Long connectionId, String activityType, String status, String title, String description) {
        logActivity(connectionId, activityType, status, title, description, null);
    }

    public void logActivity(Long connectionId, String activityType, String status,
            String title, String description, com.fasterxml.jackson.databind.JsonNode metadata) {
        try {
            ConnectionActivityLog activityLog = ConnectionActivityLog.builder()
                    .connectionId(connectionId)
                    .activityType(activityType)
                    .status(status)
                    .title(title)
                    .description(description)
                    .metadata(metadata)
                    .build();

            create(activityLog, Identifier.builder().build());
            log.debug("Logged activity: {} for connection: {}", activityType, connectionId);
        } catch (Exception e) {
            log.error("Failed to log activity for connection {}: {}", connectionId, e.getMessage());
        }
    }

    public ConnectionActivityLog getActivityLog(Identifier identifier) {
        log.info("Getting activity log by id: {}", identifier.getId());
        return activityLogDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));
    }

    public List<ConnectionActivityLog> getAllActivityLogs(Identifier identifier) {
        log.info("Getting all activity logs");
        return activityLogDao.list(identifier);
    }

    public List<ConnectionActivityLog> getActivityLogsByConnectionId(Long connectionId) {
        log.info("Getting activity logs by connection: {}", connectionId);
        return activityLogDao.listByConnectionId(connectionId);
    }

    public List<ConnectionActivityLog> getActivityLogsByType(Long connectionId, String activityType) {
        log.info("Getting activity logs by type: {} for connection: {}", activityType, connectionId);
        return activityLogDao.listByActivityType(connectionId, activityType);
    }

    public boolean deleteActivityLog(Identifier identifier) {
        log.info("Deleting activity log: {}", identifier.getId());
        ConnectionActivityLog activityLog = activityLogDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));
        return activityLogDao.delete(activityLog) > 0;
    }
}
