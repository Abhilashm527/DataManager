package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dao.IconDao;
import com.dataflow.dataloaders.entity.Icon;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.Identifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

import java.util.List;
import java.util.Optional;

@Slf4j
@Service
public class IconService {

    @Autowired
    private IconDao iconDao;

    public Icon create(Icon icon, Identifier identifier) {
        log.info("Creating icon: {}", icon.getIconName());
        return iconDao.create(icon, identifier);
    }

    /**
     * Find existing icon by name or create new icon from uploaded file
     * 
     * @param iconName   Name identifier for the icon (used for reuse)
     * @param iconFile   MultipartFile containing the icon image
     * @param identifier Request context identifier
     * @return Existing or newly created Icon
     */
    public Icon findOrCreateIconFromFile(String iconName, MultipartFile iconFile, Identifier identifier) {
        // Check if icon exists by name
        Optional<Icon> existingIcon = iconDao.getByName(iconName);

        if (existingIcon.isPresent()) {
            log.info("Reusing existing icon: {}", iconName);
            return existingIcon.get();
        }

        // Validate file before creating
        validateIconFile(iconFile);

        // Create new icon from uploaded file
        try {
            Icon newIcon = Icon.builder()
                    .iconName(iconName)
                    .iconData(iconFile.getBytes())
                    .contentType(iconFile.getContentType())
                    .fileSize(iconFile.getSize())
                    .build();

            log.info("Creating new icon: {} (size: {} bytes, type: {})",
                    iconName, iconFile.getSize(), iconFile.getContentType());
            return create(newIcon, identifier);
        } catch (IOException e) {
            log.error("Error reading icon file: {}", e.getMessage());
            throw new DataloadersException(ErrorFactory.INTERNAL_SERVER_ERROR,
                    "Failed to process icon file: " + e.getMessage());
        }
    }

    /**
     * Validate icon file for size and type
     * 
     * @param file MultipartFile to validate
     * @throws DataloadersException if validation fails
     */
    public void validateIconFile(MultipartFile file) {
        if (file == null || file.isEmpty()) {
            throw new DataloadersException(ErrorFactory.BAD_REQUEST, "Icon file is required");
        }

        // Validate file size (max 5MB)
        long maxSize = 5 * 1024 * 1024; // 5MB
        if (file.getSize() > maxSize) {
            throw new DataloadersException(ErrorFactory.BAD_REQUEST,
                    String.format("Icon file size (%d bytes) exceeds maximum limit of 5MB", file.getSize()));
        }

        // Validate content type
        String contentType = file.getContentType();
        if (contentType == null || !contentType.startsWith("image/")) {
            throw new DataloadersException(ErrorFactory.BAD_REQUEST,
                    "Icon file must be an image (received: " + contentType + ")");
        }

        log.debug("Icon file validation passed: {} bytes, type: {}", file.getSize(), contentType);
    }

    public Icon getIcon(Identifier identifier) {

        log.info("Getting icon by id: {}", identifier.getId());
        return iconDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));
    }

    public List<Icon> getAllIcons(Identifier identifier) {
        log.info("Getting all icons");
        return iconDao.list(identifier);
    }

    public Icon updateIcon(Icon icon, Identifier identifier) {
        log.info("Updating icon: {}", identifier.getId());
        Icon existing = iconDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));

        if (icon.getIconName() != null)
            existing.setIconName(icon.getIconName());
        if (icon.getIconUrl() != null)
            existing.setIconUrl(icon.getIconUrl());

        iconDao.update(existing);
        return iconDao.getV1(identifier).orElse(existing);
    }

    public boolean deleteIcon(Identifier identifier) {
        log.info("Deleting icon: {}", identifier.getId());
        Icon icon = iconDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));
        return iconDao.delete(icon) > 0;
    }
}
