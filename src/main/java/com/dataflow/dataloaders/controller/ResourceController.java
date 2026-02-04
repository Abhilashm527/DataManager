package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.entity.Resource;
import com.dataflow.dataloaders.services.ResourceService;
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
import java.util.List;
import static com.dataflow.dataloaders.config.APIConstants.RESOURCE_BASE_PATH;

@Slf4j
@RestController
@RequestMapping(RESOURCE_BASE_PATH)
@Tag(name = "Resources", description = "Resource management APIs")
public class ResourceController {

    protected static final String logPrefix = "{} : {}";

    @Autowired
    private ResourceService resourceService;

    @Operation(summary = "Create resource", description = "Create a new resource")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "201", description = "Resource created successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid input")
    })
    @PostMapping("/{resourceId}")
    public ResponseEntity<Response> create(@RequestBody Resource resource,
                                           @Parameter(description = "Resource ID") @PathVariable String resourceId,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        resource.setItemId(resourceId);
        return Response.createResponse(resourceService.create(resource, identifier));
    }

    @Operation(summary = "Get resource by ID", description = "Get resource by ID")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Resource found"),
            @ApiResponse(responseCode = "404", description = "Resource not found")
    })
    @GetMapping("/{resourceId}")
    public ResponseEntity<Response> get(@Parameter(description = "Resource ID") @PathVariable String resourceId,
                                        @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(resourceId).build();
        return Response.getResponse(resourceService.getResource(identifier));
    }

    @Operation(summary = "Get all resources", description = "Get all resources")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Resources retrieved")
    })
    @GetMapping
    public ResponseEntity<Response> getAll(@RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(resourceService.getAllResources(identifier));
    }

    @Operation(summary = "Get resources by type", description = "Get resources by resource type")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Resources retrieved")
    })
    @GetMapping("/type/{resourceType}")
    public ResponseEntity<Response> getByResourceType(@Parameter(description = "Resource type") @PathVariable String resourceType,
                                                      @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(resourceType).build();
        return Response.getResponse(resourceService.getResourcesByType(identifier));
    }

    @Operation(summary = "Get resources by item", description = "Get resources by item ID")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Resources retrieved")
    })
    @GetMapping("/item/{itemId}")
    public ResponseEntity<Response> getByItemId(@Parameter(description = "Item ID") @PathVariable String itemId,
                                                      @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(itemId).build();
        return Response.getResponse(resourceService.getResourcesByItemId(identifier));
    }

    @Operation(summary = "Update resource", description = "Update an existing resource")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Resource updated successfully"),
            @ApiResponse(responseCode = "404", description = "Resource not found")
    })
    @PutMapping("/{resourceId}")
    public ResponseEntity<Response> update(@Parameter(description = "Resource ID") @PathVariable String resourceId,
                                           @RequestBody Resource request,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(resourceId).build();
        return Response.updateResponse(resourceService.updateResource(request, identifier));
    }

    @Operation(summary = "Delete resource", description = "Delete a resource")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Resource deleted successfully"),
            @ApiResponse(responseCode = "404", description = "Resource not found")
    })
    @DeleteMapping("/{resourceId}")
    public ResponseEntity<Response> delete(@Parameter(description = "Resource ID") @PathVariable String resourceId,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(resourceId).build();
        return Response.deleteResponse(resourceService.deleteResource(identifier));
    }

    @Operation(summary = "Test resource connection", description = "Test connection to an existing resource")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Connection test result")
    })
    @PostMapping("/{resourceId}/test")
    public ResponseEntity<Response> testResource(@Parameter(description = "Resource ID") @PathVariable String resourceId,
                                                 @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(resourceId).build();
        return Response.getResponse(resourceService.testResource(identifier));
    }

    @Operation(summary = "Test new resource", description = "Test connection before saving resource")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Connection test result")
    })
    @PostMapping("/test")
    public ResponseEntity<Response> testNewResource(@RequestBody Resource resource,
                                                    @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(resourceService.testNewResource(resource, identifier));
    }

    @Operation(summary = "Bulk reorder resources", description = "Reorder multiple resources")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Resources reordered successfully")
    })
    @PostMapping("/reorder")
    public ResponseEntity<Response> bulkReorderResources(@RequestBody List<Resource> resources,
                                                         @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.updateResponse(resourceService.bulkReorderResources(resources, identifier));
    }

    @Operation(summary = "Get resource types", description = "Get supported resource types")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Resource types retrieved")
    })
    @GetMapping("/types")
    public ResponseEntity<Response> getResourceTypes(@RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(resourceService.getSupportedResourceTypes());
    }

    @Operation(summary = "Update resource status", description = "Activate or deactivate a resource")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Status updated successfully")
    })
    @PatchMapping("/{resourceId}/status")
    public ResponseEntity<Response> updateStatus(@Parameter(description = "Resource ID") @PathVariable String resourceId,
                                                 @Parameter(description = "Active status") @RequestParam Boolean isActive,
                                                 @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(resourceId).build();
        return Response.updateResponse(resourceService.updateResourceStatus(identifier, isActive));
    }
}