package com.dataflow.dataloaders.controller.Dagactivity;

import com.dataflow.dataloaders.entity.ActivityDefinition;
import com.dataflow.dataloaders.services.ActivityDefinitionService;
import com.dataflow.dataloaders.util.Identifier;
import com.dataflow.dataloaders.util.Response;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import static com.dataflow.dataloaders.config.APIConstants.DAG_ACTIVITY_PATH;

@Slf4j
@RestController
@RequestMapping(DAG_ACTIVITY_PATH + "/definitions")
@Tag(name = "Activity Definitions", description = "Endpoints for fetching UI schemas and configurations for DAG nodes")
public class ActivityDefinitionController {

    @Autowired
    private ActivityDefinitionService activityDefinitionService;

    @Operation(summary = "Create an activity definition")
    @PostMapping
    public ResponseEntity<Response> create(
            @RequestBody ActivityDefinition request,
            @RequestHeader HttpHeaders headers) {
        log.info("Creating activity definition");
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.createResponse(activityDefinitionService.create(request, identifier));
    }

    @Operation(summary = "Get all available activity definitions with pagination")
    @PostMapping("/list")
    public ResponseEntity<Response> getAllActivities(
            @RequestParam(required = false, defaultValue = "0") int page,
            @RequestParam(required = false, defaultValue = "10") int size,
            @RequestParam(required = false, defaultValue = "created_at") String sortBy,
            @RequestParam(required = false, defaultValue = "DESC") Sort.Direction direction,
            @RequestHeader HttpHeaders headers) {
        log.info("Fetching all activity definitions");
        Pageable pageable = PageRequest.of(page, size, Sort.by(direction, sortBy));
        Identifier identifier = Identifier.builder().headers(headers).pageable(pageable).word("").build();
        return Response.getResponse(activityDefinitionService.list(identifier));
    }

    @Operation(summary = "Get definition by ID")
    @GetMapping("/{id}")
    public ResponseEntity<Response> getActivityDefinition(
            @Parameter(description = "Activity Definition ID") @PathVariable String id,
            @RequestHeader HttpHeaders headers) {
        log.info("Fetching activity definition: {}", id);
        Identifier identifier = Identifier.builder().headers(headers).word(id).build();
        return Response.getResponse(activityDefinitionService.getById(identifier));
    }

    @Operation(summary = "Update activity definition")
    @PutMapping("/{id}")
    public ResponseEntity<Response> update(
            @Parameter(description = "Activity Definition ID") @PathVariable String id,
            @RequestBody ActivityDefinition request,
            @RequestHeader HttpHeaders headers) {
        log.info("Updating activity definition: {}", id);
        Identifier identifier = Identifier.builder().headers(headers).word(id).build();
        return Response.updateResponse(activityDefinitionService.update(request, identifier));
    }

    @Operation(summary = "Delete activity definition")
    @DeleteMapping("/{id}")
    public ResponseEntity<Response> delete(
            @Parameter(description = "Activity Definition ID") @PathVariable String id,
            @RequestHeader HttpHeaders headers) {
        log.info("Deleting activity definition: {}", id);
        Identifier identifier = Identifier.builder().headers(headers).word(id).build();
        return Response.deleteResponse(activityDefinitionService.delete(identifier));
    }
}
