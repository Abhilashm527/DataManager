package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dao.ConnectionDao;
import com.dataflow.dataloaders.entity.Connection;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.Identifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.List;

@Slf4j
@Service
public class ConnectionService {

    @Autowired
    private ConnectionDao connectionDao;


    public Connection create(Connection connection, Identifier identifier) {
        log.info("Creating connection: {}", connection.getConnectionName());
        Connection created = connectionDao.create(connection, identifier);
        return created;
    }

    public Connection getConnection(Identifier identifier) {
        log.info("Getting connection by id: {}", identifier.getId());
        return connectionDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));
    }

    public List<Connection> getAllConnections(Identifier identifier) {
        log.info("Getting all connections");
        return connectionDao.list(identifier);
    }

    public List<Connection> getConnectionsByApplicationId(Long userId) {
        log.info("Getting connections by user id: {}", userId);
        return connectionDao.listByUserId(userId);
    }

    public List<Connection> getConnectionsByProvider(Long providerId) {
        log.info("Getting connections by provider: {}", providerId);
        return connectionDao.listByProvider(providerId);
    }

    public Connection updateConnection(Connection connection, Identifier identifier) {
        log.info("Updating connection: {}", identifier.getId());
        Connection existing = connectionDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));

        StringBuilder changes = new StringBuilder();
        
        if (connection.getConnectionName() != null) {
            changes.append("Name changed from '").append(existing.getConnectionName())
                   .append("' to '").append(connection.getConnectionName()).append("'; ");
            existing.setConnectionName(connection.getConnectionName());
        }
        if (connection.getConfig() != null) {
            changes.append("Configuration updated; ");
            existing.setConfig(connection.getConfig());
        }
        if (connection.getSecrets() != null) {
            changes.append("Credentials updated; ");
            existing.setSecrets(connection.getSecrets());
        }
        if (connection.getUseSsl() != null) {
            changes.append("SSL setting changed to ").append(connection.getUseSsl()).append("; ");
            existing.setUseSsl(connection.getUseSsl());
        }
        if (connection.getConnectionTimeout() != null) {
            changes.append("Timeout changed to ").append(connection.getConnectionTimeout()).append("s; ");
            existing.setConnectionTimeout(connection.getConnectionTimeout());
        }
        if (connection.getIsActive() != null) {
            String status = connection.getIsActive() ? "activated" : "deactivated";
            changes.append("Connection ").append(status).append("; ");
            existing.setIsActive(connection.getIsActive());
        }
        if (connection.getLastTestStatus() != null)
            existing.setLastTestStatus(connection.getLastTestStatus());
        if (connection.getLastTestedAt() != null)
            existing.setLastTestedAt(connection.getLastTestedAt());
        if (connection.getLastUsedAt() != null)
            existing.setLastUsedAt(connection.getLastUsedAt());

        existing.setUpdatedAt(DateUtils.getUnixTimestampInUTC());
        existing.setUpdatedBy("admin");
        connectionDao.update(existing);
        return connectionDao.getV1(identifier).orElse(existing);
    }

    public boolean deleteConnection(Identifier identifier) {
        log.info("Deleting connection: {}", identifier.getId());
        Connection connection = connectionDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));
        connection.setUpdatedBy("admin");
        return connectionDao.delete(connection) > 0;
    }
}
