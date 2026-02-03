package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.entity.Deploy;
import com.dataflow.dataloaders.services.DeployService;
import com.dataflow.dataloaders.util.Identifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/v1/dataloader/deploy")
public class DeployController {

    @Autowired
    private DeployService deployService;

    @PostMapping
    public ResponseEntity<Deploy> createDeploy(@RequestBody Deploy deploy) {
        log.info("POST - deploy: {}", deploy);
        Deploy created = deployService.createDeploy(deploy, new Identifier());
        return ResponseEntity.ok(created);
    }

    @GetMapping("/{deployId}")
    public ResponseEntity<Deploy> getDeploy(@PathVariable String deployId) {
        log.info("GET /api/v1/dataloader/deploy/{}", deployId);
        Deploy deploy = deployService.getDeploy(Identifier.builder().word(deployId).build());
        return ResponseEntity.ok(deploy);
    }
    @GetMapping("/runningConfig/{jobId}")
    public ResponseEntity<Map<String, Object>> getJobConfigById(@PathVariable String jobId) {
        log.info("GET /api/v1/dataloader/deploy/{}", jobId);
        Map<String, Object> deploy = deployService.getJobConfigByJobId(Identifier.builder().word(jobId).build());
        return ResponseEntity.ok(deploy);
    }

    @GetMapping("/runningConfig/scheduleId/{schedulerId}")
    public ResponseEntity<Map<String, Object>> getJobConfigBySchedulerId(@PathVariable Long schedulerId) {
        log.info("GET /runningConfig/scheduleId/{}", schedulerId);
        Map<String, Object> deploy = deployService.getRunningConfigBySchedulerId(Identifier.builder().id(schedulerId).build());
        return ResponseEntity.ok(deploy);
    }

    @GetMapping("/app/{applicationId}")
    public ResponseEntity<List<Deploy>> getAllDeploys(@PathVariable String applicationId) {
        log.info("GET /api/v1/dataloader/deploy");
        List<Deploy> deploys = deployService.getAllDeploysByAppId(new Identifier(applicationId));
        return ResponseEntity.ok(deploys);
    }

    @GetMapping("/job/{jobId}")
    public ResponseEntity<List<Deploy>> getDeploysByJobId(@PathVariable String jobId) {
        log.info("GET /api/v1/dataloader/deploy/job/{}", jobId);
        List<Deploy> deploys = deployService.getDeploysByJobId(Identifier.builder().word(jobId).build());
        return ResponseEntity.ok(deploys);
    }

    @PutMapping("/{deployId}")
    public ResponseEntity<Deploy> updateDeploy(@PathVariable String deployId, @RequestBody Deploy deploy) {
        log.info("PUT /api/v1/dataloader/deploy/{} - deploy: {}", deployId, deploy);
        Deploy updated = deployService.updateDeploy(deploy, Identifier.builder().word(deployId).build());
        return ResponseEntity.ok(updated);
    }

    @DeleteMapping("/{deployId}")
    public ResponseEntity<Boolean> deleteDeploy(@PathVariable String deployId) {
        log.info("DELETE /api/v1/dataloader/deploy/{}", deployId);
        boolean deleted = deployService.deleteDeploy(Identifier.builder().word(deployId).build());
        return ResponseEntity.ok(deleted);
    }
}