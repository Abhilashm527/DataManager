package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.entity.Deploy;
import com.dataflow.dataloaders.services.DeployService;
import com.dataflow.dataloaders.util.Identifier;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/v1/dataloader/deploy")
@Tag(name = "Deploy", description = "Deployment management APIs")
public class DeployController {

    @Autowired
    private DeployService deployService;

    @Operation(summary = "Create deployment", description = "Create a new deployment")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Deployment created successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid input")
    })
    @PostMapping
    public ResponseEntity<Deploy> createDeploy(@RequestBody Deploy deploy) {
        log.info("POST - deploy: {}", deploy);
        Deploy created = deployService.createDeploy(deploy, new Identifier());
        return ResponseEntity.ok(created);
    }

    @Operation(summary = "Get deployment", description = "Get deployment by ID")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Deployment found"),
            @ApiResponse(responseCode = "404", description = "Deployment not found")
    })
    @GetMapping("/{deployId}")
    public ResponseEntity<Deploy> getDeploy(@Parameter(description = "Deploy ID") @PathVariable String deployId) {
        log.info("GET /api/v1/dataloader/deploy/{}", deployId);
        Deploy deploy = deployService.getDeploy(Identifier.builder().word(deployId).build());
        return ResponseEntity.ok(deploy);
    }
    
    @Operation(summary = "Get job config by job ID", description = "Get running job configuration by job ID")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Job config retrieved")
    })
    @GetMapping("/runningConfig/{jobId}")
    public ResponseEntity<Map<String, Object>> getJobConfigById(@Parameter(description = "Job ID") @PathVariable String jobId) {
        log.info("GET /api/v1/dataloader/deploy/{}", jobId);
        Map<String, Object> deploy = deployService.getJobConfigByJobId(Identifier.builder().word(jobId).build());
        return ResponseEntity.ok(deploy);
    }

    @Operation(summary = "Get job config by scheduler ID", description = "Get running job configuration by scheduler ID")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Job config retrieved")
    })
    @GetMapping("/runningConfig/scheduleId/{schedulerId}")
    public ResponseEntity<Map<String, Object>> getJobConfigBySchedulerId(@Parameter(description = "Scheduler ID") @PathVariable Long schedulerId) {
        log.info("GET /runningConfig/scheduleId/{}", schedulerId);
        Map<String, Object> deploy = deployService.getRunningConfigBySchedulerId(Identifier.builder().id(schedulerId).build());
        return ResponseEntity.ok(deploy);
    }

    @Operation(summary = "Get deployments by application", description = "Get all deployments for an application")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Deployments retrieved")
    })
    @GetMapping("/app/{applicationId}")
    public ResponseEntity<List<Deploy>> getAllDeploys(@Parameter(description = "Application ID") @PathVariable String applicationId) {
        log.info("GET /api/v1/dataloader/deploy");
        List<Deploy> deploys = deployService.getAllDeploysByAppId(new Identifier(applicationId));
        return ResponseEntity.ok(deploys);
    }

    @Operation(summary = "Get deployments by job ID", description = "Get all deployments for a job")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Deployments retrieved")
    })
    @GetMapping("/job/{jobId}")
    public ResponseEntity<List<Deploy>> getDeploysByJobId(@Parameter(description = "Job ID") @PathVariable String jobId) {
        log.info("GET /api/v1/dataloader/deploy/job/{}", jobId);
        List<Deploy> deploys = deployService.getDeploysByJobId(Identifier.builder().word(jobId).build());
        return ResponseEntity.ok(deploys);
    }

    @Operation(summary = "Update deployment", description = "Update an existing deployment")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Deployment updated successfully"),
            @ApiResponse(responseCode = "404", description = "Deployment not found")
    })
    @PutMapping("/{deployId}")
    public ResponseEntity<Deploy> updateDeploy(@Parameter(description = "Deploy ID") @PathVariable String deployId, @RequestBody Deploy deploy) {
        log.info("PUT /api/v1/dataloader/deploy/{} - deploy: {}", deployId, deploy);
        Deploy updated = deployService.updateDeploy(deploy, Identifier.builder().word(deployId).build());
        return ResponseEntity.ok(updated);
    }

    @Operation(summary = "Delete deployment", description = "Delete a deployment")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Deployment deleted successfully"),
            @ApiResponse(responseCode = "404", description = "Deployment not found")
    })
    @DeleteMapping("/{deployId}")
    public ResponseEntity<Boolean> deleteDeploy(@Parameter(description = "Deploy ID") @PathVariable String deployId) {
        log.info("DELETE /api/v1/dataloader/deploy/{}", deployId);
        boolean deleted = deployService.deleteDeploy(Identifier.builder().word(deployId).build());
        return ResponseEntity.ok(deleted);
    }
}