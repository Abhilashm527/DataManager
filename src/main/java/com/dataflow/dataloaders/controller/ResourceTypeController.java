package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.entity.ResourceType;
import com.dataflow.dataloaders.services.ResourceTypeService;
import com.dataflow.dataloaders.util.Identifier;
import com.dataflow.dataloaders.util.Response;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

import static com.dataflow.dataloaders.config.APIConstants.RESOURCE_TYPE_BASE_PATH;

@Slf4j
@RestController
@RequestMapping(RESOURCE_TYPE_BASE_PATH)
public class ResourceTypeController {

    protected static final String logPrefix = "{} : {}";

    @Autowired
    private ResourceTypeService resourceTypeService;

    /**
     * Create a new resource type
     */
    @PostMapping
    public ResponseEntity<Response> create(@RequestBody ResourceType resourceType,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.createResponse(resourceTypeService.create(resourceType, identifier));
    }

    /**
     * Get a resource type by ID
     */
    @GetMapping("/{resourceTypeId}")
    public ResponseEntity<Response> get(@PathVariable String resourceTypeId,
                                        @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(resourceTypeId).build();
        return Response.getResponse(resourceTypeService.getResourceType(identifier));
    }

    /**
     * Get a resource type by type name
     */
    @GetMapping("/name/{typeName}")
    public ResponseEntity<Response> getByTypeName(@PathVariable String typeName,
                                                  @RequestHeader HttpHeaders headers,
                                                  @RequestParam (required = false) String type) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(typeName).build();
        return Response.getResponse(resourceTypeService.getResourceTypeByName(identifier,type));
    }

    /**
     * Get all resource types
     */
    @GetMapping
    public ResponseEntity<Response> getAll(@RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(resourceTypeService.getAllResourceTypes(identifier));
    }

    /**
     * Get all active resource types (for frontend dropdown)
     */
    @GetMapping("/active")
    public ResponseEntity<Response> getAllActive(@RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(resourceTypeService.getActiveResourceTypes());
    }

    /**
     * Update a resource type
     */
    @PutMapping("/{resourceTypeId}")
    public ResponseEntity<Response> update(@PathVariable String resourceTypeId,
                                           @RequestBody ResourceType request,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(resourceTypeId).build();
        return Response.updateResponse(resourceTypeService.updateResourceType(request, identifier));
    }

    /**
     * Delete a resource type
     */
    @DeleteMapping("/{resourceTypeId}")
    public ResponseEntity<Response> delete(@PathVariable String resourceTypeId,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(resourceTypeId).build();
        return Response.deleteResponse(resourceTypeService.deleteResourceType(identifier));
    }

    /**
     * Activate/Deactivate a resource type
     */
    @PatchMapping("/{resourceTypeId}/status")
    public ResponseEntity<Response> updateStatus(@PathVariable String resourceTypeId,
                                                 @RequestParam Boolean isActive,
                                                 @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(resourceTypeId).build();
        return Response.updateResponse(resourceTypeService.updateResourceTypeStatus(identifier, isActive));
    }

    /**
     * Bulk reorder resource types
     */
    @PostMapping("/reorder")
    public ResponseEntity<Response> bulkReorder(@RequestBody List<ResourceType> resourceTypes,
                                                @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.updateResponse(resourceTypeService.bulkReorderResourceTypes(resourceTypes, identifier));
    }


}