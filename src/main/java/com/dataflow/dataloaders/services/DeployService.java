package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.client.SchedulerRestClient;
import com.dataflow.dataloaders.dao.DeployDao;
import com.dataflow.dataloaders.dao.JobConfigDao;
import com.dataflow.dataloaders.dto.JobScheduledResponse;
import com.dataflow.dataloaders.entity.*;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.jobconfigs.*;
import com.dataflow.dataloaders.util.DateUtils;
import com.dataflow.dataloaders.util.Identifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.swing.text.html.Option;
import java.util.*;

@Slf4j
@Service
public class DeployService {

    @Autowired
    private DeployDao deployDao;
    @Autowired
    private ActivityLogService activityLogService;
    @Autowired
    private JobConfigDao jobConfigDao;
    @Autowired
    private JobConfigService jobConfigService;
    @Autowired
    private MappingsService mappingsService;
    @Autowired
    private SchedulerRestClient schedulerRestClient;
    @Autowired
    private ResourceService resourceService;

    @Transactional
    public Deploy createDeploy(Deploy deploy, Identifier identifier) {
        log.info("createDeploy - deploy: {}", deploy.getJobId());
        identifier.setWord(deploy.getJobId());
        Optional<JobConfig> publishedConfig = jobConfigDao.getV1(identifier);
        if(publishedConfig.isEmpty()){
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, "The jobId version is not found to deploy");
        }
        JobConfig jobConfig = publishedConfig.get();
        if(!deploy.isManualRun()){
            jobConfig.setScheduled(true);
            jobConfig.setSchedule(deploy.getScheduleExpression());
            Map<String, Object> deployJobBundle = buildScheduleConfig(deploy);
            JobScheduledResponse jobScheduledResponse = schedulerRestClient.scheduleJob(deployJobBundle);
            deploy.setSchedulerId(jobScheduledResponse.getJobId());
            deploy.setSchedulerName(jobScheduledResponse.getJobName());;
        }
        jobConfig.setDeployed(true);
        jobConfig.setDeployedVersion(jobConfig.getPublishedVersion());
        jobConfig.setPublished(false);
        jobConfig.setDeployedVersion(jobConfig.getDeployedVersion());
        jobConfig.setStatus("DEPLOYED");
        jobConfigService.updateJobConfig(jobConfig,identifier);
        log.info("Status has been updated to DEPLOYED: {}", jobConfig.getJobId());

        deploy.setCreatedBy("admin");
        deploy.setCreatedAt(DateUtils.getUnixTimestampInUTC());
        
        Optional<Deploy> created = deployDao.createV1(deploy, identifier);
        if (created.isEmpty()) {
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, "Failed to create deploy");
        }
        
        Deploy createdDeploy = created.get();
        activityLogService.logDataflowActivity("DEPLOY_CREATED", createdDeploy.getDeployId(), 
                "Deploy-" + createdDeploy.getDeployId(), "admin");
        
        return createdDeploy;
    }

    private Map<String, Object> buildScheduleConfig(Deploy deploy) {
        Map<String, Object> config = new HashMap<>();
        addIfNotNull(config, "configName", deploy.getDeployName());
        addIfNotNull(config, "scheduled", true);
        addIfNotNull(config, "schedule", deploy.getScheduleExpression());
        return config;
    }

    private Map<String, Object> buildDeployeConfig(JobConfig jobConfig, Deploy deploy) {
        Map<String, Object> config = new HashMap<>();
        
        addIfNotNull(config, "configName", deploy.getDeployName());
        addIfNotNull(config, "chunkSize", jobConfig.getChunkSize());
        addIfNotNull(config, "scheduled", true);
        addIfNotNull(config, "schedule", deploy.getScheduleExpression());
        
        // Job Notes
        Map<String, Object> jobNotes = new HashMap<>();
        addIfNotNull(jobNotes, "jobDescription", jobConfig.getJobDescription());
        addIfNotNull(jobNotes, "severity", jobConfig.getJobSeverity());
        addIfNotNull(jobNotes, "impacts", Collections.singletonList(jobConfig.getImpacts()));
        if (!jobNotes.isEmpty()) {
            config.put("jobNotes", jobNotes);
        }
        
        // Reader Config
        Map<String, Object> readerConfig = buildReaderConfigMap(jobConfig);
        if (!readerConfig.isEmpty()) {
            config.put("readerConfig", readerConfig);
        }
        
        // Writer Config
        Map<String, Object> writerConfig = buildWriterConfigMap(jobConfig);
        if (!writerConfig.isEmpty()) {
            config.put("writerConfig", writerConfig);
        }
        
        // Input Fields
        Mappings mappings = mappingsService.getMapping(new Identifier(jobConfig.getMappingId()));
        if (mappings.getMappings() != null && !mappings.getMappings().isEmpty()) {
            config.put("inputFields", mappings.getMappings());
        }
        if(jobConfig.getSourceConfig().getSourceId() != null){
            Map<String,Object> readerConfig1 = (Map<String,Object>) config.get("readerConfig");
            readerConfig1.put("connectionConfig", getresourcebyId(jobConfig.getSourceConfig().getSourceId()));
        }
        if(jobConfig.getTargetConfig().getTargetId() != null){
            Map<String,Object> writerConfig1 = (Map<String,Object>) config.get("writerConfig");
            writerConfig1.put("connectionConfig", getresourcebyId(jobConfig.getTargetConfig().getTargetId()));
        }
        
        return config;
    }
    private Map<String,Object> getresourcebyId(String id) {
        Resource resource = resourceService.getResource(new Identifier(id));
        return resource.getConfiguration();
    }
    
    private void addIfNotNull(Map<String, Object> map, String key, Object value) {
        if (value != null) {
            map.put(key, value);
        }
    }


    public Deploy getDeploy(Identifier identifier) {
        log.info("getDeploy - identifier: {}", identifier);
        return deployDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));
    }

    public List<Deploy> getAllDeploys(Identifier identifier) {
        log.info("getAllDeploys");
        return deployDao.list(identifier);
    }

    public List<Deploy> getDeploysByJobId(Identifier identifier) {
        log.info("getDeploysByJobId - jobId: {}", identifier.getWord());
        return deployDao.getByJobId(identifier);
    }

    @Transactional
    public Deploy updateDeploy(Deploy deploy, Identifier identifier) {
        log.info("updateDeploy - identifier: {}", identifier);
        Deploy existing = deployDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));

        if (deploy.getJobId() != null) existing.setJobId(deploy.getJobId());
        if (deploy.getParentJobId() != null) existing.setParentJobId(deploy.getParentJobId());
        existing.setManualRun(deploy.isManualRun());
        existing.setSchedule(deploy.isSchedule());
        if (deploy.getScheduleExpression() != null) existing.setScheduleExpression(deploy.getScheduleExpression());
        if (deploy.getSchedulerId() != null) existing.setSchedulerId(deploy.getSchedulerId());
        existing.setActive(deploy.isActive());
        existing.setUpdatedBy("admin");
        existing.setUpdatedAt(DateUtils.getUnixTimestampInUTC());

        deployDao.updateDeployStatus(existing, identifier);
        return existing;
    }

    @Transactional
    public boolean deleteDeploy(Identifier identifier) {
        log.info("deleteDeploy - identifier: {}", identifier);
        Deploy deploy = deployDao.getV1(identifier)
                .orElseThrow(() -> new DataloadersException(ErrorFactory.RESOURCE_NOT_FOUND));

        deployDao.delete(deploy);
        activityLogService.logDataflowActivity("DEPLOY_DELETED", deploy.getDeployId(),
                "Deploy-" + deploy.getDeployId(), "admin");

        return true;
    }
    private void setReaderConfigFields(com.dataflow.dataloaders.jobconfigs.ReaderConfig readerConfig, Map<String, Object> fields) {
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (value == null) continue;

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

    private Map<String, Object> buildReaderConfigMap(JobConfig jobConfig) {
        Map<String, Object> readerConfig = new HashMap<>();
        
        if(jobConfig.getSourceConfig().getConfigFields() != null) {
            addNonNullFields(readerConfig, jobConfig.getSourceConfig().getConfigFields());
        }
        if(jobConfig.getSourceConfig().getPredefinedFields() != null) {
            addNonNullFields(readerConfig, jobConfig.getSourceConfig().getPredefinedFields());
        }
        
        return readerConfig;
    }

    private Map<String, Object> buildWriterConfigMap(JobConfig jobConfig) {
        Map<String, Object> writerConfig = new HashMap<>();
        
        if(jobConfig.getTargetConfig().getConfigFields() != null) {
            addNonNullFields(writerConfig, jobConfig.getTargetConfig().getConfigFields());
        }
        if(jobConfig.getTargetConfig().getPredefinedFields() != null) {
            addNonNullFields(writerConfig, jobConfig.getTargetConfig().getPredefinedFields());
        }
        
        return writerConfig;
    }

    private void addNonNullFields(Map<String, Object> config, Map<String, Object> fields) {
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            if (entry.getValue() != null) {
                config.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public List<Deploy> getAllDeploysByAppId(Identifier identifier) {
        log.info( this.getClass().getSimpleName(), "getAllJobConfigs - identifier: {}", identifier);
        List<Deploy> deploy = deployDao.getAllDeployByappId(identifier);
        if (deploy.isEmpty()) {
            log.warn("No deployed jobs found for identifier: {}", identifier);
            return Collections.emptyList();
        }
        log.info("Found {} job configs", deploy.size());
        return deploy;
    }

    public Map<String, Object> getRunningConfigBySchedulerId(Identifier identifier) {
        Optional<Deploy> deploy =  deployDao.getBySchedulerId(identifier);
        Deploy deploy1 = deploy.isPresent() ? deploy.get() : null;
        if (deploy1 == null) return Collections.emptyMap();
        Identifier identifier1 = new Identifier();
        identifier1.setWord(deploy1.getJobId());
        return getJobConfigByJobId(identifier1);
    }

    public Map<String, Object> getJobConfigByJobId(Identifier identifier) {
        log.info("createDeploy - deploy: {}", identifier.getWord());
        Optional<JobConfig> publishedConfig = jobConfigDao.getV1(identifier);
        List<Deploy> deployList = getDeploysByJobId(identifier);
        if(publishedConfig.isEmpty()){
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, "The jobId version is not found to deploy");
        }
        JobConfig jobConfig = publishedConfig.get();
        Deploy deploy = deployList!=null ? deployList.get(0) : null;
        return buildDeployeConfig(jobConfig, deploy);
    }
}