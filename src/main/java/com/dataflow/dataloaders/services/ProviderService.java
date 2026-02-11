package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dao.ProviderDao;
import com.dataflow.dataloaders.entity.Provider;
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
public class ProviderService {

    @Autowired
    private ProviderDao providerDao;

    @Autowired
    private IconService iconService;

    public Provider create(Provider request, Identifier identifier) {
        Provider provider = Provider.builder()
                .connectionTypeId(request.getConnectionTypeId())
                .providerName(request.getProviderName())
                .description(request.getDescription())
                .iconId(request.getIconId())
                .defaultPort(request.getDefaultPort())
                .configSchema(request.getConfigSchema())
                .displayOrder(request.getDisplayOrder())
                .build();

        log.info("Creating provider: {}", request.getProviderName());
        return providerDao.create(provider, identifier);
    }

    public Provider getProvider(Identifier identifier) {
        log.info("Getting provider by id: {}", identifier.getWord());
        return providerDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));
    }

    public List<Provider> getAllProviders(Identifier identifier) {
        log.info("Getting all providers");
        return providerDao.list(identifier);
    }

    public List<Provider> getProvidersByConnectionType(String connectionTypeId) {
        log.info("Getting providers by connection type: {}", connectionTypeId);
        return providerDao.listByConnectionType(connectionTypeId);
    }

    public Provider updateProvider(Provider provider, Identifier identifier) {
        log.info("Updating provider: {}", identifier.getWord());
        Provider existing = providerDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));

        if (provider.getDescription() != null)
            existing.setDescription(provider.getDescription());
        if (provider.getIconId() != null)
            existing.setIconId(provider.getIconId());
        if (provider.getDefaultPort() != null)
            existing.setDefaultPort(provider.getDefaultPort());
        if (provider.getConfigSchema() != null)
            existing.setConfigSchema(provider.getConfigSchema());
        if (provider.getDisplayOrder() != null)
            existing.setDisplayOrder(provider.getDisplayOrder());

        existing.setUpdatedBy("admin");
        existing.setUpdatedAt(DateUtils.getUnixTimestampInUTC());

        providerDao.update(existing);
        return providerDao.getV1(identifier).orElse(existing);
    }

    public boolean deleteProvider(Identifier identifier) {
        log.info("Deleting provider: {}", identifier.getWord());
        Provider provider = providerDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));
        provider.setUpdatedBy("admin");
        return providerDao.delete(provider) > 0;
    }
}
