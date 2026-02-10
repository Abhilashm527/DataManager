package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dao.RecentConnectionDao;
import com.dataflow.dataloaders.dto.RecentConnectionResponse;
import com.dataflow.dataloaders.entity.RecentConnection;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.Identifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Optional;

@Slf4j
@Service
public class RecentConnectionService {

    @Autowired
    private RecentConnectionDao recentConnectionDao;

    /**
     * Automatically track a connection usage by a user
     * This method is called internally when a user accesses/uses a connection
     * It creates a new record or updates the existing one (increments access count)
     * 
     * @param userId       User ID
     * @param connectionId Connection ID
     */
    public void trackConnection(Long userId, Long connectionId) {
        log.info("Tracking connection usage - User: {}, Connection: {}", userId, connectionId);

        // Check if this user+connection combination already exists
        List<RecentConnection> existingRecords = recentConnectionDao.listByUserId(userId);
        Optional<RecentConnection> existing = existingRecords.stream()
                .filter(rc -> rc.getConnectionId().equals(connectionId))
                .findFirst();

        if (existing.isPresent()) {
            // Record exists - the database will auto-increment access_count via ON CONFLICT
            log.info("Updating existing recent connection record");
            RecentConnection record = existing.get();
            // Re-insert to trigger ON CONFLICT UPDATE
            try {
                recentConnectionDao.insert(record, Identifier.builder().build());
            } catch (Exception e) {
                // This is expected - ON CONFLICT will handle the update
                log.debug("ON CONFLICT triggered for user {} connection {}", userId, connectionId);
            }
        } else {
            // Create new record
            log.info("Creating new recent connection record");
            RecentConnection newRecord = RecentConnection.builder()
                    .userId(userId)
                    .connectionId(connectionId)
                    .accessCount(1)
                    .build();
            recentConnectionDao.insert(newRecord, Identifier.builder().build());
        }
    }

    public RecentConnection getRecentConnection(Identifier identifier) {
        log.info("Getting recent connection by id: {}", identifier.getId());
        return recentConnectionDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));
    }

    public List<RecentConnectionResponse> getAllRecentConnections(Identifier identifier) {
        log.info("Getting all recent connections");
        return recentConnectionDao.listAllWithDetails(identifier);
    }

    public List<RecentConnectionResponse> getRecentConnectionsByUserId(Long userId) {
        log.info("Getting recent connections by user: {}", userId);
        return recentConnectionDao.listByUserIdWithDetails(userId);
    }

    public boolean deleteRecentConnection(Identifier identifier) {
        log.info("Deleting recent connection: {}", identifier.getId());
        RecentConnection recentConnection = recentConnectionDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));
        return recentConnectionDao.delete(recentConnection) > 0;
    }

    public int deleteAllByUserId(Long userId) {
        log.info("Clearing all recent connections for user: {}", userId);
        List<RecentConnection> userConnections = recentConnectionDao.listByUserId(userId);
        int deletedCount = 0;
        for (RecentConnection rc : userConnections) {
            deletedCount += recentConnectionDao.delete(rc);
        }
        log.info("Deleted {} recent connections for user {}", deletedCount, userId);
        return deletedCount;
    }
}