package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dao.ConnectionTypeDao;
import com.dataflow.dataloaders.dto.ConnectionTypeRequest;
import com.dataflow.dataloaders.entity.ConnectionType;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.Identifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

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
     * @param iconFile   Optional icon file upload
     * @param identifier Request context
     * @return Created ConnectionType
     */
    public ConnectionType create(ConnectionTypeRequest request, MultipartFile iconFile, Identifier identifier) {
        Long iconId = null;

        // Handle icon if provided
        if (iconFile != null && !iconFile.isEmpty()) {
            if (request.getIconName() == null || request.getIconName().trim().isEmpty()) {
                throw new DataloadersException(ErrorFactory.BAD_REQUEST,
                        "iconName is required when uploading an icon file");
            }
            iconService.validateIconFile(iconFile);
            var icon = iconService.findOrCreateIconFromFile(request.getIconName(), iconFile, identifier);
            iconId = icon.getId();
        }

        ConnectionType connectionType = ConnectionType.builder()
                .typeKey(request.getTypeKey())
                .displayName(request.getDisplayName())
                .iconId(iconId)
                .displayOrder(request.getDisplayOrder())
                .build();

        log.info("Creating connection type: {} with icon ID: {}", request.getTypeKey(), iconId);
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

        if (connectionType.getDisplayName() != null)
            existing.setDisplayName(connectionType.getDisplayName());
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
