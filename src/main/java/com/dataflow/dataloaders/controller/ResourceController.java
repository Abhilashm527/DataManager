package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.entity.Resource;
import com.dataflow.dataloaders.services.ResourceService;
import com.dataflow.dataloaders.util.Identifier;
import com.dataflow.dataloaders.util.Response;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import static com.dataflow.dataloaders.config.APIConstants.RESOURCE_BASE_PATH;

@Slf4j
@RestController
@RequestMapping(RESOURCE_BASE_PATH)
public class ResourceController {

    protected static final String logPrefix = "{} : {}";

    @Autowired
    private ResourceService resourceService;

    /**
     * Create a new resource
     */
    @PostMapping("/{resourceId}")
    public ResponseEntity<Response> create(@RequestBody Resource resource,
                                           @PathVariable String resourceId,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        resource.setItemId(resourceId);
        return Response.createResponse(resourceService.create(resource, identifier));
    }

    /**
     * Get a resource by ID
     */
    @GetMapping("/{resourceId}")
    public ResponseEntity<Response> get(@PathVariable String resourceId,
                                        @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(resourceId).build();
        return Response.getResponse(resourceService.getResource(identifier));
    }

    /**
     * Get all resources
     */
    @GetMapping
    public ResponseEntity<Response> getAll(@RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(resourceService.getAllResources(identifier));
    }

    /**
     * Get resources by resource type
     */
    @GetMapping("/type/{resourceType}")
    public ResponseEntity<Response> getByResourceType(@PathVariable String resourceType,
                                                      @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(resourceType).build();
        return Response.getResponse(resourceService.getResourcesByType(identifier));
    }

    /**
     * Get resources by item_id
     */
    @GetMapping("/item/{itemId}")
    public ResponseEntity<Response> getByItemId(@PathVariable String itemId,
                                                      @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(itemId).build();
        return Response.getResponse(resourceService.getResourcesByItemId(identifier));
    }

    /**
     * Update a resource
     */
    @PutMapping("/{resourceId}")
    public ResponseEntity<Response> update(@PathVariable String resourceId,
                                           @RequestBody Resource request,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(resourceId).build();
        return Response.updateResponse(resourceService.updateResource(request, identifier));
    }

    /**
     * Delete a resource
     */
    @DeleteMapping("/{resourceId}")
    public ResponseEntity<Response> delete(@PathVariable String resourceId,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(resourceId).build();
        return Response.deleteResponse(resourceService.deleteResource(identifier));
    }

    /**
     * Test a resource connection
     */
    @PostMapping("/{resourceId}/test")
    public ResponseEntity<Response> testResource(@PathVariable String resourceId,
                                                 @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(resourceId).build();
        return Response.getResponse(resourceService.testResource(identifier));
    }

    /**
     * Test resource connection before saving (validate resource parameters)
     */
    @PostMapping("/test")
    public ResponseEntity<Response> testNewResource(@RequestBody Resource resource,
                                                    @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(resourceService.testNewResource(resource, identifier));
    }

    /**
     * Bulk reorder resources
     */
    @PostMapping("/reorder")
    public ResponseEntity<Response> bulkReorderResources(@RequestBody List<Resource> resources,
                                                         @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.updateResponse(resourceService.bulkReorderResources(resources, identifier));
    }

    /**
     * Get supported resource types with their configuration fields
     */
    @GetMapping("/types")
    public ResponseEntity<Response> getResourceTypes(@RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(resourceService.getSupportedResourceTypes());
    }

    /**
     * Activate/Deactivate a resource
     */
    @PatchMapping("/{resourceId}/status")
    public ResponseEntity<Response> updateStatus(@PathVariable String resourceId,
                                                 @RequestParam Boolean isActive,
                                                 @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(resourceId).build();
        return Response.updateResponse(resourceService.updateResourceStatus(identifier, isActive));
    }
}