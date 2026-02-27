package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dao.ActivityDefinitionDao;
import com.dataflow.dataloaders.entity.ActivityDefinition;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.Identifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class ActivityDefinitionService {

    @Autowired
    private ActivityDefinitionDao activityDefinitionDao;

    public ActivityDefinition create(ActivityDefinition request, Identifier identifier) {
        log.info("Creating activity definition: {}", request.getActivityType());
        return activityDefinitionDao.createV1(request, identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.DATABASE_EXCEPTION));
    }

    public ActivityDefinition getById(Identifier identifier) {
        log.info("Getting activity definition by id: {}", identifier.getWord());
        return activityDefinitionDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));
    }

    public Page<ActivityDefinition> list(Identifier identifier) {
        log.info("Listing activity definitions");
        List<ActivityDefinition> items = activityDefinitionDao.list(identifier);
        long total = items.isEmpty() ? 0 : (items.get(0).getTotal() != null ? items.get(0).getTotal() : items.size());
        return new org.springframework.data.domain.PageImpl<>(items, identifier.getPageable(), total);
    }

    public ActivityDefinition update(ActivityDefinition request, Identifier identifier) {
        log.info("Updating activity definition: {}", identifier.getWord());
        ActivityDefinition existing = getById(identifier);

        if (request.getActivityType() != null)
            existing.setActivityType(request.getActivityType());
        if (request.getCategory() != null)
            existing.setCategory(request.getCategory());
        if (request.getLabel() != null)
            existing.setLabel(request.getLabel());
        if (request.getDescription() != null)
            existing.setDescription(request.getDescription());
        if (request.getIconStr() != null)
            existing.setIconStr(request.getIconStr());
        if (request.getSupportedConnectionTypes() != null)
            existing.setSupportedConnectionTypes(request.getSupportedConnectionTypes());
        if (request.getConfigSchema() != null)
            existing.setConfigSchema(request.getConfigSchema());

        existing.setUpdatedBy("admin");
        activityDefinitionDao.update(existing);
        return getById(identifier);
    }

    public boolean delete(Identifier identifier) {
        log.info("Deleting activity definition: {}", identifier.getWord());
        ActivityDefinition existing = getById(identifier);
        existing.setUpdatedBy("admin");
        return activityDefinitionDao.delete(existing) > 0;
    }
}
