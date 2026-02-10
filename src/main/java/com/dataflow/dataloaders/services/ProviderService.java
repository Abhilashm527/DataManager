package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dao.ProviderDao;
import com.dataflow.dataloaders.dto.ProviderRequest;
import com.dataflow.dataloaders.entity.Provider;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.Identifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

@Slf4j
@Service
public class ProviderService {

    @Autowired
    private ProviderDao providerDao;

    @Autowired
    private IconService iconService;

    public Provider create(ProviderRequest request, MultipartFile iconFile, Identifier identifier) {
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

        Provider provider = Provider.builder()
                .connectionTypeId(request.getConnectionTypeId())
                .providerKey(request.getProviderKey())
                .displayName(request.getDisplayName())
                .iconId(iconId)
                .defaultPort(request.getDefaultPort())
                .configSchema(request.getConfigSchema())
                .displayOrder(request.getDisplayOrder())
                .build();

        log.info("Creating provider: {} with icon ID: {}", request.getProviderKey(), iconId);
        return create(provider, identifier);
    }

    public Provider create(Provider provider, Identifier identifier) {

        log.info("Creating provider: {}", provider.getProviderKey());
        return providerDao.create(provider, identifier);
    }

    public Provider getProvider(Identifier identifier) {
        log.info("Getting provider by id: {}", identifier.getId());
        return providerDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));
    }

    public List<Provider> getAllProviders(Identifier identifier) {
        log.info("Getting all providers");
        return providerDao.list(identifier);
    }

    public List<Provider> getProvidersByConnectionType(Long connectionTypeId) {
        log.info("Getting providers by connection type: {}", connectionTypeId);
        return providerDao.listByConnectionType(connectionTypeId);
    }

    public Provider updateProvider(Provider provider, Identifier identifier) {
        log.info("Updating provider: {}", identifier.getId());
        Provider existing = providerDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));

        if (provider.getDisplayName() != null)
            existing.setDisplayName(provider.getDisplayName());
        if (provider.getIconId() != null)
            existing.setIconId(provider.getIconId());
        if (provider.getDefaultPort() != null)
            existing.setDefaultPort(provider.getDefaultPort());
        if (provider.getConfigSchema() != null)
            existing.setConfigSchema(provider.getConfigSchema());
        if (provider.getDisplayOrder() != null)
            existing.setDisplayOrder(provider.getDisplayOrder());

        providerDao.update(existing);
        return providerDao.getV1(identifier).orElse(existing);
    }

    public boolean deleteProvider(Identifier identifier) {
        log.info("Deleting provider: {}", identifier.getId());
        Provider provider = providerDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));
        return providerDao.delete(provider) > 0;
    }
}
