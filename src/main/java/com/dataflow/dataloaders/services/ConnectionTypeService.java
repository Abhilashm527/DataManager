package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dao.ConnectionTypeDao;
import com.dataflow.dataloaders.entity.ConnectionType;
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
public class ConnectionTypeService {

    @Autowired
    private ConnectionTypeDao connectionTypeDao;

    @Autowired
    private IconService iconService;

    /**
     * Create ConnectionType with multipart icon file upload
     * 
     * @param request    ConnectionTypeRequest DTO
     * @param identifier Request context
     * @return Created ConnectionType
     */
    public ConnectionType create(ConnectionType request, Identifier identifier) {
        Long iconId = null;
        ConnectionType connectionType = ConnectionType.builder()
                .connectionType(request.getConnectionType())
                .iconId(request.getIconId())
                .displayOrder(request.getDisplayOrder())
                .build();

        log.info("Creating connection type: {} with icon ID: {}", request.getConnectionType(), iconId);
        return connectionTypeDao.create(connectionType, identifier);
    }

    public ConnectionType getConnectionType(Identifier identifier) {
        log.info("Getting connection type by id: {}", identifier.getId());
        return connectionTypeDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));
    }

    public List<ConnectionType> getAllConnectionTypes(Identifier identifier) {
        log.info("Getting all connection types");
        return connectionTypeDao.list(identifier);
    }

    public ConnectionType updateConnectionType(ConnectionType connectionType, Identifier identifier) {
        log.info("Updating connection type: {}", identifier.getId());
        ConnectionType existing = connectionTypeDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));

        if (connectionType.getIconId() != null)
            existing.setIconId(connectionType.getIconId());
        if (connectionType.getDisplayOrder() != null)
            existing.setDisplayOrder(connectionType.getDisplayOrder());

        existing.setUpdatedBy("admin");
        existing.setUpdatedAt(DateUtils.getUnixTimestampInUTC());

        connectionTypeDao.update(existing);
        return connectionTypeDao.getV1(identifier).orElse(existing);
    }

    public boolean deleteConnectionType(Identifier identifier) {
        log.info("Deleting connection type: {}", identifier.getId());
        ConnectionType connectionType = connectionTypeDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));
        connectionType.setUpdatedBy("admin");
        return connectionTypeDao.delete(connectionType) > 0;
    }
}
