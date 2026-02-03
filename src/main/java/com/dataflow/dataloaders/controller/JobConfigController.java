package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.dto.PublishRequest;
import com.dataflow.dataloaders.entity.JobConfig;
import com.dataflow.dataloaders.services.JobConfigService;
import com.dataflow.dataloaders.util.Identifier;
import com.dataflow.dataloaders.util.Response;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import static com.dataflow.dataloaders.config.APIConstants.JOB_CONFIG_BASE_PATH;

@Slf4j
@RestController
@RequestMapping(JOB_CONFIG_BASE_PATH)
public class JobConfigController {

    protected static final String logPrefix = "{} : {}";

    @Autowired
    private JobConfigService jobConfigService;

    @PostMapping("/draft")
    public ResponseEntity<Response> saveDraft(@RequestBody JobConfig jobConfig,
                                              @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), "saveDraft");
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.createResponse(jobConfigService.saveDraft(jobConfig, identifier));
    }

    @PostMapping("/publish")
    public ResponseEntity<Response> publish(@RequestBody JobConfig jobConfig,
                                            @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), "publish");
        Identifier identifier = Identifier.builder().headers(headers).word(jobConfig.getJobId()).build();
        return Response.createResponse(jobConfigService.publish(jobConfig, identifier));
    }

    @PostMapping("/deploy/{jobId}")
    public ResponseEntity<Response> deploy(@PathVariable String jobId,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), "deploy");
        Identifier identifier = Identifier.builder().headers(headers).word(jobId).build();
        return Response.createResponse(jobConfigService.deploy(identifier));
    }

    @GetMapping("/{jobId}")
    public ResponseEntity<Response> get(@PathVariable String jobId,
                                        @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(jobId).build();
        return Response.getResponse(jobConfigService.getJobConfig(identifier));
    }

    @GetMapping("/dataflow/{itemId}")
    public ResponseEntity<Response> getAllByItemId(@PathVariable String itemId,@RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(itemId).build();
        return Response.getResponse(jobConfigService.getAllConfigsByItemId(identifier));
    }

    @GetMapping("/parent/{jobId}")
    public ResponseEntity<Response> getByParent(@PathVariable String jobId,
                                        @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(jobId).build();
        return Response.getResponse(jobConfigService.getJobConfigByParent(identifier));
    }


    @GetMapping
    public ResponseEntity<Response> getAll(@RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(jobConfigService.getAllJobConfigs(identifier));
    }

    @PutMapping("/{jobId}")
    public ResponseEntity<Response> update(@PathVariable String jobId,
                                           @RequestBody JobConfig request,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(jobId).build();
        return Response.updateResponse(jobConfigService.updateJobConfig(request, identifier));
    }

    @DeleteMapping("/{jobId}")
    public ResponseEntity<Response> delete(@PathVariable String jobId,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(jobId).build();
        return Response.deleteResponse(jobConfigService.deleteJobConfig(identifier));
    }

    @DeleteMapping("/dataflow/{parentId}")
    public ResponseEntity<Response> deleteInItemlist(@PathVariable String parentId,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(parentId).build();
        return Response.deleteResponse(jobConfigService.deleteItem(identifier));
    }

    @PatchMapping("/{jobId}/status")
    public ResponseEntity<Response> updateStatus(@PathVariable String jobId,
                                                 @RequestParam Boolean isActive,
                                                 @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(jobId).build();
        return Response.updateResponse(jobConfigService.updateJobConfigStatus(identifier, isActive));
    }
    

    
    @GetMapping("/reader-configs")
    public ResponseEntity<Response> getReaderConfigs(@RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        return Response.getResponse(jobConfigService.getSupportedReaderConfigs());
    }
    
    @GetMapping("/writer-configs")
    public ResponseEntity<Response> getWriterConfigs(@RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        return Response.getResponse(jobConfigService.getSupportedWriterConfigs());
    }
    
    @GetMapping("/reader-configs/{resourceType}")
    public ResponseEntity<Response> getReaderConfigDetails(@PathVariable String resourceType,
                                                          @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        return Response.getResponse(jobConfigService.getReaderConfigDetails(resourceType));
    }
    
    @GetMapping("/writer-configs/{resourceType}")
    public ResponseEntity<Response> getWriterConfigDetails(@PathVariable String resourceType,
                                                          @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        return Response.getResponse(jobConfigService.getWriterConfigDetails(resourceType));
    }
}