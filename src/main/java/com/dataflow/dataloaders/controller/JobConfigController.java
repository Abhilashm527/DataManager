package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.dto.PublishRequest;
import com.dataflow.dataloaders.entity.JobConfig;
import com.dataflow.dataloaders.services.JobConfigService;
import com.dataflow.dataloaders.util.Identifier;
import com.dataflow.dataloaders.util.Response;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import static com.dataflow.dataloaders.config.APIConstants.JOB_CONFIG_BASE_PATH;

@Slf4j
@RestController
@RequestMapping(JOB_CONFIG_BASE_PATH)
@Tag(name = "Job Config", description = "Job configuration management APIs")
public class JobConfigController {

    protected static final String logPrefix = "{} : {}";

    @Autowired
    private JobConfigService jobConfigService;

    @Operation(summary = "Save draft", description = "Save job configuration as draft")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "201", description = "Draft saved successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid input")
    })
    @PostMapping("/draft")
    public ResponseEntity<Response> saveDraft(@RequestBody JobConfig jobConfig,
                                              @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), "saveDraft");
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.createResponse(jobConfigService.saveDraft(jobConfig, identifier));
    }

    @Operation(summary = "Publish job config", description = "Publish job configuration")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Job config published successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid input")
    })
    @PostMapping("/publish")
    public ResponseEntity<Response> publish(@RequestBody JobConfig jobConfig,
                                            @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), "publish");
        Identifier identifier = Identifier.builder().headers(headers).word(jobConfig.getJobId()).build();
        return Response.createResponse(jobConfigService.publish(jobConfig, identifier));
    }

    @Operation(summary = "Deploy job", description = "Deploy a job configuration")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Job deployed successfully"),
            @ApiResponse(responseCode = "404", description = "Job not found")
    })
    @PostMapping("/deploy/{jobId}")
    public ResponseEntity<Response> deploy(@Parameter(description = "Job ID") @PathVariable String jobId,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), "deploy");
        Identifier identifier = Identifier.builder().headers(headers).word(jobId).build();
        return Response.createResponse(jobConfigService.deploy(identifier));
    }

    @Operation(summary = "Get job config", description = "Get job configuration by ID")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Job config found"),
            @ApiResponse(responseCode = "404", description = "Job config not found")
    })
    @GetMapping("/{jobId}")
    public ResponseEntity<Response> get(@Parameter(description = "Job ID") @PathVariable String jobId,
                                        @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(jobId).build();
        return Response.getResponse(jobConfigService.getJobConfig(identifier));
    }

    @Operation(summary = "Get job configs by item", description = "Get all job configurations for an item")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Job configs retrieved")
    })
    @GetMapping("/dataflow/{itemId}")
    public ResponseEntity<Response> getAllByItemId(@Parameter(description = "Item ID") @PathVariable String itemId,@RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(itemId).build();
        return Response.getResponse(jobConfigService.getAllConfigsByItemId(identifier));
    }

    @Operation(summary = "Get job configs by parent", description = "Get job configurations by parent job ID")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Job configs retrieved")
    })
    @GetMapping("/parent/{jobId}")
    public ResponseEntity<Response> getByParent(@Parameter(description = "Parent Job ID") @PathVariable String jobId,
                                        @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(jobId).build();
        return Response.getResponse(jobConfigService.getJobConfigByParent(identifier));
    }

    @Operation(summary = "Get all job configs", description = "Get all job configurations")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Job configs retrieved")
    })
    @GetMapping
    public ResponseEntity<Response> getAll(@RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(jobConfigService.getAllJobConfigs(identifier));
    }

    @Operation(summary = "Update job config", description = "Update job configuration")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Job config updated successfully"),
            @ApiResponse(responseCode = "404", description = "Job config not found")
    })
    @PutMapping("/{jobId}")
    public ResponseEntity<Response> update(@Parameter(description = "Job ID") @PathVariable String jobId,
                                           @RequestBody JobConfig request,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(jobId).build();
        return Response.updateResponse(jobConfigService.updateJobConfig(request, identifier));
    }

    @Operation(summary = "Delete job config", description = "Delete job configuration")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Job config deleted successfully"),
            @ApiResponse(responseCode = "404", description = "Job config not found")
    })
    @DeleteMapping("/{jobId}")
    public ResponseEntity<Response> delete(@Parameter(description = "Job ID") @PathVariable String jobId,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(jobId).build();
        return Response.deleteResponse(jobConfigService.deleteJobConfig(identifier));
    }

    @Operation(summary = "Delete item", description = "Delete item and associated job configs")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Item deleted successfully")
    })
    @DeleteMapping("/dataflow/{parentId}")
    public ResponseEntity<Response> deleteInItemlist(@Parameter(description = "Parent ID") @PathVariable String parentId,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(parentId).build();
        return Response.deleteResponse(jobConfigService.deleteItem(identifier));
    }

    @Operation(summary = "Update job config status", description = "Update job configuration status")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Status updated successfully")
    })
    @PatchMapping("/{jobId}/status")
    public ResponseEntity<Response> updateStatus(@Parameter(description = "Job ID") @PathVariable String jobId,
                                                 @Parameter(description = "Active status") @RequestParam Boolean isActive,
                                                 @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(jobId).build();
        return Response.updateResponse(jobConfigService.updateJobConfigStatus(identifier, isActive));
    }
    
    @Operation(summary = "Get reader configs", description = "Get supported reader configurations")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Reader configs retrieved")
    })
    @GetMapping("/reader-configs")
    public ResponseEntity<Response> getReaderConfigs(@RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        return Response.getResponse(jobConfigService.getSupportedReaderConfigs());
    }
    
    @Operation(summary = "Get writer configs", description = "Get supported writer configurations")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Writer configs retrieved")
    })
    @GetMapping("/writer-configs")
    public ResponseEntity<Response> getWriterConfigs(@RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        return Response.getResponse(jobConfigService.getSupportedWriterConfigs());
    }
    
    @Operation(summary = "Get reader config details", description = "Get reader configuration details by resource type")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Reader config details retrieved")
    })
    @GetMapping("/reader-configs/{resourceType}")
    public ResponseEntity<Response> getReaderConfigDetails(@Parameter(description = "Resource type") @PathVariable String resourceType,
                                                          @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        return Response.getResponse(jobConfigService.getReaderConfigDetails(resourceType));
    }
    
    @Operation(summary = "Get writer config details", description = "Get writer configuration details by resource type")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Writer config details retrieved")
    })
    @GetMapping("/writer-configs/{resourceType}")
    public ResponseEntity<Response> getWriterConfigDetails(@Parameter(description = "Resource type") @PathVariable String resourceType,
                                                          @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        return Response.getResponse(jobConfigService.getWriterConfigDetails(resourceType));
    }
}