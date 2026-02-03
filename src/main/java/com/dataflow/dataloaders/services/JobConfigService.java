package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dao.JobConfigDao;
import com.dataflow.dataloaders.dao.JobConfigReferenceDao;
import com.dataflow.dataloaders.dto.SourceConfig;
import com.dataflow.dataloaders.dto.TargetConfig;
import com.dataflow.dataloaders.entity.JobConfig;
import com.dataflow.dataloaders.entity.JobConfigReference;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.jobconfigs.DeployJobBundle;
import com.dataflow.dataloaders.jobconfigs.JobNotes;
import com.dataflow.dataloaders.jobconfigs.ReaderConfig;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.Identifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
public class JobConfigService {

    protected static final String logPrefix = "{} : {}";

    @Autowired
    private JobConfigDao jobConfigDao;

    @Autowired
    private JobConfigReferenceDao jobConfigReferenceDao;
    
    @Autowired
    private MappingsService mappingsService;

    @Autowired
    private ResourceService resourceService;

    @Autowired
    private ActivityLogService activityLogService;

    @Transactional
    public JobConfig saveDraft(JobConfig jobConfig, Identifier identifier) {
        log.info("saveDraft - identifier: {}", identifier);
        if (jobConfig.getItemId() == null ){
            throw new DataloadersException(ErrorFactory.BAD_REQUEST, "ItemId could not be null");
        }
        if(jobConfig.getSourceConfig().getSourceType().equalsIgnoreCase("DATATABLE")){
            SourceConfig sourceConfig = setSourceFieldsForDatatables();
            jobConfig.setSourceConfig(sourceConfig);
        }
        if(jobConfig.getTargetConfig().getTargetType().equalsIgnoreCase("DATATABLE")){
            TargetConfig targetConfig = setTargetFieldsForDatatables();
            jobConfig.setTargetConfig(targetConfig);
        }
        jobConfig.setStatus("DRAFT");
        jobConfig.setDrafted(true);
        jobConfig.setIsActive(false);
        jobConfig.setPublished(false);
        jobConfig.setDeployed(false);
        jobConfig.setCreatedBy("admin");
        jobConfig.setCreatedAt(DateUtils.getUnixTimestampInUTC());
        Optional<JobConfig> created = jobConfigDao.createV1(jobConfig, identifier);
        if (created.isEmpty()) {
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, "Failed to create draft");
        }
        JobConfig createdJobConfig = created.get();
        createdJobConfig.setParentJobId(createdJobConfig.getJobId());
        jobConfigDao.updateJobConfigParentJobId(createdJobConfig,identifier);
        JobConfigReference jobConfigReference = new JobConfigReference();
        jobConfigReference.setJobId(createdJobConfig.getJobId());
        jobConfigReference.setItemId(createdJobConfig.getItemId());
        jobConfigReference.setCreatedBy("admin");
        jobConfigReferenceDao.createV1(jobConfigReference,identifier);
        log.info("Created draft job config: {}", created.get().getJobId());
        
        // Log activity
        activityLogService.logDataflowActivity("DRAFT_CREATED", createdJobConfig.getJobId(), 
                createdJobConfig.getJobName(), "admin");
        
        return createdJobConfig;
    }

    private TargetConfig setTargetFieldsForDatatables() {
        Map<String, Object> predefinedFields = new HashMap<>();
        predefinedFields.put("writer","mongo");
        predefinedFields.put("writerType","db");
        predefinedFields.put("mongoDatabase","datatable_db");
        predefinedFields.put("mongoCollection","data_table_records");
        TargetConfig targetConfig = new TargetConfig();
        targetConfig.setTargetType("Mongo");
        targetConfig.setTargetId("eZV4eM_fLf2vxXThGTdM9g");
        targetConfig.setPredefinedFields(predefinedFields);
        return targetConfig;
    }

    private SourceConfig setSourceFieldsForDatatables() {
        Map<String, Object> predefinedFields = new HashMap<>();
        predefinedFields.put("reader","mongo");
        predefinedFields.put("readerType","db");
        predefinedFields.put("readerBuilder","mongo");
        predefinedFields.put("mongoDatabase","datatable_db");
        predefinedFields.put("mongoCollection","data_table_records");
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setSourceType("Mongo");
        sourceConfig.setSourceId("eZV4eM_fLf2vxXThGTdM9g");
        sourceConfig.setPredefinedFields(predefinedFields);
        return sourceConfig;
    }

    @Transactional
    public JobConfig publish(JobConfig jobConfig, Identifier identifier) {
        log.info("publish - jobConfig: {}", jobConfig.getJobId());

        // If jobId is provided, publish from existing record
        if (jobConfig.getJobId() != null) {
            JobConfig sourceConfig = jobConfigDao.getV1(identifier)
                    .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));

            validateSourceDestinationCompatibility(jobConfig);
            applyDefaultValues(jobConfig);
            
            // Create new published record from existing
            JobConfig publishedConfig = copyJobConfig(sourceConfig);
            publishedConfig.setJobId(null); // Generate new ID
            String parentId = sourceConfig.getParentJobId() != null ? sourceConfig.getParentJobId() : sourceConfig.getJobId();
            publishedConfig.setParentJobId(parentId);
            publishedConfig.setPublishedVersion(generateNextVersion(getLatestPublishedVersion(parentId)));
            publishedConfig.setPublished(true);
            publishedConfig.setStatus("PUBLISHED");
            publishedConfig.setCreatedBy("admin");
            publishedConfig.setCreatedAt(DateUtils.getUnixTimestampInUTC());

            Optional<JobConfig> created = jobConfigDao.createV1(publishedConfig, identifier);
            if (created.isEmpty()) {
                throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, "Failed to create published version");
            }
            log.info("Published job config: {} with version: {}", created.get().getJobId(), publishedConfig.getPublishedVersion());
            
            // Log activity
            activityLogService.logDataflowActivity("PUBLISHED", created.get().getJobId(), 
                    publishedConfig.getJobName(), "admin");
            
            return created.get();
        } else {
            // Direct publish - create new record
            validateSourceDestinationCompatibility(jobConfig);
            applyDefaultValues(jobConfig);
            Identifier parentIdentifier = new Identifier();
            if (jobConfig.getParentJobId() == null) {
                JobConfig draftedJobConfig = saveDraft(jobConfig,identifier);
                jobConfig.setParentJobId(draftedJobConfig.getJobId());
                jobConfig.setPublishedVersion("v1.0");
                jobConfig.setJobId(null);
                jobConfig.setDrafted(false);
            } else {
                parentIdentifier.setWord(jobConfig.getParentJobId());
                JobConfig sourceConfig = jobConfigDao.getLatestByParentId(parentIdentifier)
                        .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND,"Provided parent Id is not matching or exists"));
                jobConfig.setJobName(sourceConfig.getJobName());
                jobConfig.setPublishedVersion(generateNextVersion(getLatestPublishedVersion(sourceConfig.getParentJobId())));
            }
            jobConfig.setPublished(true);
            jobConfig.setStatus("PUBLISHED");
            jobConfig.setCreatedBy("admin");
            jobConfig.setDeployedVersion(null);
            jobConfig.setDeployed(false);
            jobConfig.setCreatedAt(DateUtils.getUnixTimestampInUTC());
            Optional<JobConfig> created = jobConfigDao.createV1(jobConfig, identifier);
            if (created.isEmpty()) {
                throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, "Failed to create published version");
            }
            log.info("Published job config directly: {} with version: {}", created.get().getJobId(), jobConfig.getPublishedVersion());
            
            // Log activity
            activityLogService.logDataflowActivity("PUBLISHED", created.get().getJobId(), 
                    jobConfig.getJobName(), "admin");
            
            return created.get();
        }
    }

    @Transactional
    public JobConfig deploy(Identifier identifier) {
        log.info("deploy - jobId: {}", identifier.getWord());
        
        // Find published version to deploy
        Optional<JobConfig> publishedConfig = jobConfigDao.getV1(identifier);
        if(publishedConfig.isEmpty()){
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, "The jobId version is not found to deploy");
        }
        JobConfig jobConfig = publishedConfig.get();


        String parentJobId = jobConfig.getParentJobId();
        String version = generateNextVersion(getLatestDeployedVersion(parentJobId));
        
        DeployJobBundle deployJobConfig = new DeployJobBundle();
        deployJobConfig.setConfigName(jobConfig.getJobName());
        JobNotes jobNotes = new JobNotes();
        jobNotes.setJobDescription(jobConfig.getJobDescription());
        jobNotes.setSeverity(JobNotes.SeverityEnum.valueOf(jobConfig.getJobSeverity()));
        jobNotes.setImpacts(Collections.singletonList(jobConfig.getImpacts()));
        deployJobConfig.setJobNotes(jobNotes);
        deployJobConfig.setChunkSize(jobConfig.getChunkSize());
        
        // Set ReaderConfig with all fields from SourceConfig
        com.dataflow.dataloaders.jobconfigs.ReaderConfig readerConfig = new com.dataflow.dataloaders.jobconfigs.ReaderConfig();
        
        // Set fields from configFields (user-configurable)
        if(jobConfig.getSourceConfig().getConfigFields() != null) {
            setReaderConfigFields(readerConfig, jobConfig.getSourceConfig().getConfigFields());
        }
        
        // Set fields from predefinedFields (system-managed)
        if(jobConfig.getSourceConfig().getPredefinedFields() != null) {
            setReaderConfigFields(readerConfig, jobConfig.getSourceConfig().getPredefinedFields());
        }
        
        deployJobConfig.setReaderConfig(readerConfig);
        
        // Set WriterConfig with all fields from TargetConfig
        com.dataflow.dataloaders.jobconfigs.WriterConfig writerConfig = new com.dataflow.dataloaders.jobconfigs.WriterConfig();
        
        // Set fields from configFields (user-configurable)
        if(jobConfig.getTargetConfig().getConfigFields() != null) {
            setWriterConfigFields(writerConfig, jobConfig.getTargetConfig().getConfigFields());
        }
        
        // Set fields from predefinedFields (system-managed)
        if(jobConfig.getTargetConfig().getPredefinedFields() != null) {
            setWriterConfigFields(writerConfig, jobConfig.getTargetConfig().getPredefinedFields());
        }
        
        deployJobConfig.setWriterConfig(writerConfig);

        // Create new deployed record
        JobConfig deployedConfig = copyJobConfig(jobConfig);
        deployedConfig.setJobId(null); // Generate new ID
        deployedConfig.setParentJobId(parentJobId);
        deployedConfig.setDeployedVersion(version);
        deployedConfig.setDeployed(true);
        deployedConfig.setStatus("DEPLOYED");
        deployedConfig.setIsActive(true);
        deployedConfig.setCreatedBy("admin");
        deployedConfig.setCreatedAt(DateUtils.getUnixTimestampInUTC());

        Optional<JobConfig> created = jobConfigDao.createV1(deployedConfig, new Identifier());
        if (created.isEmpty()) {
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, "Failed to create deployed version");
        }
        log.info("Deployed job config: {} with version: {}", created.get().getJobId(), version);
        
        // Log activity
        activityLogService.logDataflowActivity("DEPLOYED", created.get().getJobId(), 
                deployedConfig.getJobName(), "admin");
        
        return created.get();
    }

    public JobConfig getJobConfig(Identifier identifier) {
        log.info(logPrefix, this.getClass().getSimpleName(), "getJobConfig - identifier: {}", identifier);
        JobConfig jobConfig = jobConfigDao.getV1(identifier)
                .orElseThrow(() -> {
                    log.warn("JobConfig not found for identifier: {}", identifier);
                    return new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND);
                });
        return jobConfig;
    }

    public List<JobConfigReference> getAllConfigsByItemId(Identifier identifier) {
        log.info(logPrefix, this.getClass().getSimpleName(), "getAllJobConfigs - identifier: {}", identifier);
        List<JobConfigReference> jobConfigReferences = jobConfigReferenceDao.getByItemId(identifier);
        if (jobConfigReferences.isEmpty()) {
            log.warn("No job configs found for identifier: {}", identifier);
            return Collections.emptyList();
        }
        log.info("Found {} job configs", jobConfigReferences.size());
        return jobConfigReferences;
    }

    public List<JobConfig> getAllJobConfigs(Identifier identifier) {
        log.info(logPrefix, this.getClass().getSimpleName(), "getAllJobConfigs - identifier: {}", identifier);
        List<JobConfig> jobConfigs = jobConfigDao.list(identifier);
        if (jobConfigs.isEmpty()) {
            log.warn("No job configs found for identifier: {}", identifier);
            return Collections.emptyList();
        }
        log.info("Found {} job configs", jobConfigs.size());
        return jobConfigs;
    }

    public List<JobConfig> getJobConfigByParent(Identifier identifier) {
        log.info(logPrefix, this.getClass().getSimpleName(), "getJobConfig - identifier: {}", identifier);
        List<JobConfig> jobConfig = jobConfigDao.getByParentId(identifier);
        if (jobConfig.isEmpty()) {
            log.warn("No job configs found for identifier: {}", identifier);
            return Collections.emptyList();
        }
        return jobConfig;
    }

    @Transactional
    public Object deleteJobConfig(Identifier identifier) {
        log.info("deleteJobConfig - identifier: {}", identifier);
        JobConfig jobConfig = jobConfigDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));

        jobConfigDao.delete(jobConfig);
        log.info("Deleted job config: {}", identifier);
        return true;
    }
    public boolean deleteItem(Identifier identifier) {
        Optional<JobConfigReference> jobConfigReferences = jobConfigReferenceDao.getByJobId(identifier.getWord());
        if (jobConfigReferences.isEmpty()) {
            log.warn("item not found - identifier: {}", identifier);
            throw new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND);
        }
        boolean done = jobConfigReferenceDao.deleteById(jobConfigReferences.get().getId()) == 1;
        if(done){
            int size = jobConfigDao.deleteByParentId(identifier);
            log.info("deleted these :{} jobId from parentId :{}",size,identifier.getWord());
        }
        return true;
    }

    public JobConfig updateJobConfig(JobConfig jobConfig, Identifier identifier) {
        log.info("updateJobConfig - identifier: {}, jobConfig: {}", identifier, jobConfig);
        Optional<JobConfig> existingJobConfig = jobConfigDao.getV1(identifier);
        if (existingJobConfig.isEmpty()) {
            log.warn("JobConfig not found for update - identifier: {}", identifier);
            throw new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND);
        }

        JobConfig existing = existingJobConfig.get();

        // Update basic fields
        if (jobConfig.getJobName() != null) {
            existing.setJobName(jobConfig.getJobName());
        }
        if (jobConfig.getJobDescription() != null) {
            existing.setJobDescription(jobConfig.getJobDescription());
        }
        if (jobConfig.getImpacts() != null) {
            existing.setImpacts(jobConfig.getImpacts());
        }
        if (jobConfig.getJobSeverity() != null) {
            existing.setJobSeverity(jobConfig.getJobSeverity());
        }
        if (jobConfig.getChunkSize() != null) {
            existing.setChunkSize(jobConfig.getChunkSize());
        }
        if (jobConfig.getMappingId() != null) {
            existing.setMappingId(jobConfig.getMappingId());
        }
        if (jobConfig.getSourceConfig() != null) {
            existing.setSourceConfig(jobConfig.getSourceConfig());
        }
        if (jobConfig.getTargetConfig() != null) {
            existing.setTargetConfig(jobConfig.getTargetConfig());
        }
        if (jobConfig.getSchedule() != null) {
            existing.setSchedule(jobConfig.getSchedule());
        }
        if(jobConfig.getScheduled() != null) {
            existing.setScheduled(jobConfig.getScheduled());
        }
        if(jobConfig.getPublished() != null) {
            existing.setPublished(jobConfig.getPublished());
        }
        if (jobConfig.getPublishedVersion() != null) {
            existing.setPublishedVersion(jobConfig.getPublishedVersion());
        }
        if (jobConfig.getDeployedVersion() != null) {
            existing.setDeployedVersion(jobConfig.getDeployedVersion());
        }
        if(jobConfig.getDeployed() != null) {
            existing.setDeployed(jobConfig.getDeployed());
        }
        if(jobConfig.getStatus() != null) {
            existing.setStatus(jobConfig.getStatus());
        }
        existing.setUpdatedBy("admin");
        existing.setUpdatedAt(DateUtils.getUnixTimestampInUTC());

        boolean updated = jobConfigDao.updateJobConfig(existing, identifier) > 0;
        if (!updated) {
            log.error("Failed to update job config: {}", existing.getJobId());
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, "Not updated");
        }
        log.info("Updated job config: {}", existing.getJobId());
        return jobConfigDao.getV1(identifier).orElse(existing);
    }

    public JobConfig updateJobConfigStatus(Identifier identifier, Boolean isActive) {
        log.info("updateJobConfigStatus - identifier: {}, isActive: {}", identifier, isActive);
        JobConfig jobConfig = jobConfigDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));

        jobConfig.setIsActive(isActive);
        jobConfig.setStatus(isActive ? "ACTIVE" : "INACTIVE");
        jobConfig.setUpdatedBy("admin");
        jobConfig.setUpdatedAt(DateUtils.getUnixTimestampInUTC());

        jobConfigDao.updateJobConfig(jobConfig, identifier);
        log.info("Updated job config status: {} to {}", jobConfig.getJobId(), isActive);
        return jobConfig;
    }

    private void validateSourceDestinationCompatibility(JobConfig jobConfig) {
        // Validate mapping exists using existing service
        if (jobConfig.getMappingId() != null) {
            try {
                mappingsService.getMapping(Identifier.builder().word(jobConfig.getMappingId()).build());
            } catch (Exception e) {
                throw new DataloadersException(ErrorFactory.VALIDATION_ERROR, 
                        "Mapping not found: " + jobConfig.getMappingId());
            }
        }
        
        // Validate source resource exists
        if (jobConfig.getSourceConfig() != null && jobConfig.getSourceConfig().getSourceId() != null) {
            try {
                var resource = resourceService.getResource(Identifier.builder().word(jobConfig.getSourceConfig().getSourceId()).build());
                jobConfig.getSourceConfig().setSourceType(resource.getResourceType());
            } catch (Exception e) {
                throw new DataloadersException(ErrorFactory.VALIDATION_ERROR, 
                        "Source resource not found: " + jobConfig.getSourceConfig().getSourceId());
            }
        }
        
        // Validate target resource exists
        if (jobConfig.getTargetConfig() != null && jobConfig.getTargetConfig().getTargetId() != null) {
            try {
                var resource = resourceService.getResource(Identifier.builder().word(jobConfig.getTargetConfig().getTargetId()).build());
                jobConfig.getTargetConfig().setTargetType(resource.getResourceType());
            } catch (Exception e) {
                throw new DataloadersException(ErrorFactory.VALIDATION_ERROR, 
                        "Target resource not found: " + jobConfig.getTargetConfig().getTargetId());
            }
        }
    }
    
    private JobConfig copyJobConfig(JobConfig source) {
        JobConfig copy = new JobConfig();
        copy.setJobName(source.getJobName());
        copy.setJobDescription(source.getJobDescription());
        copy.setImpacts(source.getImpacts());
        copy.setJobSeverity(source.getJobSeverity());
        copy.setChunkSize(source.getChunkSize());
        copy.setMappingId(source.getMappingId());
        copy.setSourceConfig(source.getSourceConfig());
        copy.setTargetConfig(source.getTargetConfig());
        copy.setScheduled(source.getScheduled());
        copy.setSchedule(source.getSchedule());
        copy.setItemId(source.getItemId());
        return copy;
    }
    
    private String getLatestPublishedVersion(String parentJobId) {
        return jobConfigDao.getLatestPublishedVersion(parentJobId).orElse(null);
    }
    
    private String getLatestDeployedVersion(String parentJobId) {
        return jobConfigDao.getLatestDeployedVersion(parentJobId).orElse(null);
    }
    
    private String generateNextVersion(String currentVersion) {
        if (currentVersion == null || currentVersion.isEmpty()) {
            return "v1.0";
        }
        
        String[] parts = currentVersion.replace("v", "").split("\\.");
        int major = Integer.parseInt(parts[0]);
        int minor = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
        
        return "v" + major + "." + (minor + 1);
    }
    
    private void applyDefaultValues(JobConfig jobConfig) {
        // Apply default predefined fields to source config
        if (jobConfig.getSourceConfig() != null) {
            if(jobConfig.getSourceConfig().getSourceType().equalsIgnoreCase("SFTP")){
                Map<String, Object> predefinedFields = new HashMap<>();
                predefinedFields.put("reader", "csv");
                predefinedFields.put("fileLocation", "sftp");
                jobConfig.getSourceConfig().setPredefinedFields(predefinedFields);
            } else if (jobConfig.getSourceConfig().getSourceType().equalsIgnoreCase("PostgreSQL")){
                Map<String, Object> predefinedFields = new HashMap<>();
                predefinedFields.put("readerType", "db");
                predefinedFields.put("reader", "jdbc");
                predefinedFields.put("readerBuilder", "jdbcCursor");
                predefinedFields.put("readerName", "jdbcCursor");
                jobConfig.getSourceConfig().setPredefinedFields(predefinedFields);
            } else {
                Map<String, Object> predefinedFields = new HashMap<>();
                predefinedFields.put("readerType", "db");
                predefinedFields.put("reader", "jdbc");
                predefinedFields.put("readerBuilder", "JdbcReaderBuilder");
                jobConfig.getSourceConfig().setPredefinedFields(predefinedFields);
            }
        }
        
        // Apply default predefined fields to target config
        if (jobConfig.getTargetConfig() != null) {
            if(jobConfig.getTargetConfig().getTargetType().equalsIgnoreCase("SFTP")){
                Map<String, Object> predefinedFields = new HashMap<>();
                predefinedFields.put("writer", "csv");
                predefinedFields.put("fileLocation", "sftp");
                predefinedFields.put("writerBuilder", "sftp");
                predefinedFields.put("writerName", "sftp");
                predefinedFields.put("writerType", "db");
                jobConfig.getTargetConfig().setPredefinedFields(predefinedFields);
            } else if (jobConfig.getTargetConfig().getTargetType().equalsIgnoreCase("PostgreSQL")){
                Map<String, Object> predefinedFields = new HashMap<>();
                predefinedFields.put("writer", "jdbc");
                predefinedFields.put("writerBuilder", "jdbc");
                predefinedFields.put("writerName", "jdbc");
                predefinedFields.put("writerType", "db");
                jobConfig.getTargetConfig().setPredefinedFields(predefinedFields);
            } else {
                Map<String, Object> predefinedFields = new HashMap<>();
                predefinedFields.put("writerType", "file");
                predefinedFields.put("writer", "csv");
                predefinedFields.put("writerBuilder", "CsvWriterBuilder");
                jobConfig.getTargetConfig().setPredefinedFields(predefinedFields);
            }
        }
    }

    public Map<String, Object> getSupportedReaderConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("jdbc", Map.of("readerType", "db", "reader", "jdbc", "readerBuilder", "JdbcReaderBuilder"));
        configs.put("csv", Map.of("readerType", "file", "reader", "csv", "readerBuilder", "CsvReaderBuilder"));
        return configs;
    }

    public Map<String, Object> getSupportedWriterConfigs() {
        Map<String, Object> configs = new HashMap<>();
        configs.put("jdbc", Map.of("writerType", "db", "writer", "jdbc", "writerBuilder", "JdbcWriterBuilder"));
        configs.put("csv", Map.of("writerType", "file", "writer", "csv", "writerBuilder", "CsvWriterBuilder"));
        return configs;
    }

    public Map<String, Object> getReaderConfigDetails(String resourceType) {
        Map<String, Object> config = new HashMap<>();
        if ("MySQL".equalsIgnoreCase(resourceType) || "PostgreSQL".equalsIgnoreCase(resourceType) || "Oracle".equalsIgnoreCase(resourceType)) {
            config.put("readerType", "db");
            config.put("reader", "jdbc");
            config.put("readerBuilder", "JdbcReaderBuilder");
        } else if ("SFTP".equalsIgnoreCase(resourceType)) {
            config.put("readerType", "file");
            config.put("reader", "csv");
            config.put("readerBuilder", "CsvReaderBuilder");
        }
        return config;
    }

    public Map<String, Object> getWriterConfigDetails(String resourceType) {
        Map<String, Object> config = new HashMap<>();
        if ("MySQL".equalsIgnoreCase(resourceType) || "PostgreSQL".equalsIgnoreCase(resourceType) || "Oracle".equalsIgnoreCase(resourceType)) {
            config.put("writerType", "db");
            config.put("writer", "jdbc");
            config.put("writerBuilder", "JdbcWriterBuilder");
        } else if ("SFTP".equalsIgnoreCase(resourceType)) {
            config.put("writerType", "file");
            config.put("writer", "csv");
            config.put("writerBuilder", "CsvWriterBuilder");
        }
        return config;
    }
    
    private void setReaderConfigFields(com.dataflow.dataloaders.jobconfigs.ReaderConfig readerConfig, Map<String, Object> fields) {
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            
            switch (key) {
                case "reader": readerConfig.setReader((String) value); break;
                case "readerType": readerConfig.setReaderType((String) value); break;
                case "readerBuilder": readerConfig.setReaderBuilder((String) value); break;
                case "tableName": readerConfig.setTableName((String) value); break;
                case "connectionName": readerConfig.setConnectionName((String) value); break;
                case "inputFile": readerConfig.setInputFile((String) value); break;
                case "delimiter": readerConfig.setDelimiter((String) value); break;
                case "linesToSkip": readerConfig.setLinesToSkip((Integer) value); break;
                case "selectClause": readerConfig.setSelectClause((String) value); break;
                case "fromClause": readerConfig.setFromClause((String) value); break;
                case "whereClause": readerConfig.setWhereClause((String) value); break;
                case "sftpRemoteDir": readerConfig.setSftpRemoteDir((String) value); break;
                case "sftpFileName": readerConfig.setSftpFileName((String) value); break;
                case "sftpFileType": readerConfig.setSftpFileType((String) value); break;
            }
        }
    }
    
    private void setWriterConfigFields(com.dataflow.dataloaders.jobconfigs.WriterConfig writerConfig, Map<String, Object> fields) {
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            
            switch (key) {
                case "writer": writerConfig.setWriter((String) value); break;
                case "writerType": writerConfig.setWriterType((String) value); break;
                case "writerBuilder": writerConfig.setWriterBuilder((String) value); break;
                case "tableName": writerConfig.setTableName((String) value); break;
                case "connectionName": writerConfig.setConnectionName((String) value); break;
                case "outputFile": writerConfig.setOutputFile((String) value); break;
                case "delimiter": writerConfig.setDelimiter((String) value); break;
                case "sftpRemoteDir": writerConfig.setSftpRemoteDir((String) value); break;
                case "sftpFileName": writerConfig.setSftpFileName((String) value); break;
                case "sftpMoveDir": writerConfig.setSftpMoveDir((String) value); break;
                case "endPointUrl": writerConfig.setEndPointUrl((String) value); break;
                case "requestMethod": writerConfig.setRequestMethod((String) value); break;
            }
        }
    }

}