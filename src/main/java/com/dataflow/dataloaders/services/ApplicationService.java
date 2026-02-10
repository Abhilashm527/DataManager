package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dao.ApplicationDao;
import com.dataflow.dataloaders.entity.Application;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.Identifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class ApplicationService {

    protected static final String logPrefix = "{} : {}";

    @Autowired
    private ApplicationDao applicationDao;

    public Application create(Application application, Identifier identifier) {
        log.info(logPrefix, this.getClass().getSimpleName(), "create - identifier: {}", identifier);
        Application created = applicationDao.create(application, identifier);
        log.info("Created application: {}", created);
        return created;
    }

    public Application getApplication(Identifier identifier) {
        log.info(logPrefix, this.getClass().getSimpleName(), "getApplication - identifier: {}", identifier);
        Application application = applicationDao.getV1(identifier)
                .orElseThrow(() -> {
                    log.warn("Application not found for identifier: {}", identifier);
                    return new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND);
                });
        log.info("Found application: {}", application.getId());
        return application;
    }

    public List<Application> getAllApplications(Identifier identifier) {
        log.info(logPrefix, this.getClass().getSimpleName(), "getAllApplications - identifier: {}", identifier);
        List<Application> applications = applicationDao.list(identifier);
        if (applications.isEmpty()) {
            log.warn("No applications found for identifier: {}", identifier);
            throw new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND);
        }
        log.info("Found {} applications", applications.size());
        return applications;
    }

    public Application updateApplication(Application application, Identifier identifier) {
        log.info("updateApplication - identifier: {}, application: {}", identifier, application);
        Application existing = applicationDao.getV1(identifier)
                .orElseThrow(() -> {
                    log.warn("Application not found for update - identifier: {}", identifier);
                    return new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND);
                });

        existing.setName(application.getName() != null ? application.getName() : existing.getName());
        existing.setEnvironment(application.getEnvironment() != null ? application.getEnvironment() : existing.getEnvironment());
        existing.setDescription(application.getDescription() != null ? application.getDescription() : existing.getDescription());
        existing.setUpdatedBy("admin");

        boolean updated = applicationDao.updateApp(existing, identifier) > 0;
        if (!updated) {
            log.error("Failed to update application: {}", existing.getId());
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, "Not updated");
        }
        log.info("Updated application: {}", existing.getId());
        return getApplication(identifier);
    }

    public Object deleteApplication(Identifier identifier) {
        log.info("deleteApplication - identifier: {}", identifier);
        Application application = getApplication(identifier);
        boolean deleted = applicationDao.delete(application) > 0;
        if (!deleted) {
            log.error("Failed to delete application: {}", application.getId());
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, "Not deleted");
        }
        log.info("Deleted application: {}", identifier);
        return true;
    }
}