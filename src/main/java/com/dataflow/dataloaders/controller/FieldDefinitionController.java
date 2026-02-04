package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.entity.FieldDefinition;
import com.dataflow.dataloaders.enums.Provider;
import com.dataflow.dataloaders.services.FieldDefinitionService;
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

import static com.dataflow.dataloaders.config.APIConstants.CONFIG_FIELDS_BASE_PATH;

@Slf4j
@RestController
@RequestMapping(CONFIG_FIELDS_BASE_PATH)
@Tag(name = "Field Definitions", description = "Field Definition management APIs")
public class FieldDefinitionController {

    protected static final String logPrefix = "{} : {}";

    @Autowired
    private FieldDefinitionService fieldDefinitionService;

    @Operation(summary = "Create field definition", description = "Create a new field definition")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "201", description = "Field definition created successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid input")
    })
    @PostMapping
    public ResponseEntity<Response> create(@RequestBody FieldDefinition fieldDefinition,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.createResponse(fieldDefinitionService.create(fieldDefinition, identifier));
    }

    @Operation(summary = "Get field definition by ID", description = "Retrieve a field definition by its ID")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Field definition found"),
            @ApiResponse(responseCode = "404", description = "Field definition not found")
    })
    @GetMapping("/{configFieldId}")
    public ResponseEntity<Response> get(@Parameter(description = "Field definition ID") @PathVariable String configFieldId,
                                        @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(configFieldId).build();
        return Response.getResponse(fieldDefinitionService.getFieldDefinition(identifier));
    }

    /**
     * Get a resource type by type name
     */
    @GetMapping("/name/{typeName}")
    public ResponseEntity<Response> getByTypeNameOld(@PathVariable String typeName,
                                                  @RequestHeader HttpHeaders headers,
                                                  @RequestParam (required = false) String type) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(typeName).build();
        return Response.getResponse(fieldDefinitionService.getByTypeName(typeName));
    }

    @Operation(summary = "Get all field definitions", description = "Get all field definitions with optional provider filter")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Field definitions retrieved successfully")
    })
    @GetMapping
    public ResponseEntity<Response> getAll(@Parameter(description = "Filter by provider") @RequestParam(required = false) Provider provider,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        if (provider != null) {
            return Response.getResponse(fieldDefinitionService.getByProvider(provider));
        }
        return Response.getResponse(fieldDefinitionService.getAllFieldDefinitions(identifier));
    }

    @Operation(summary = "Get field definitions by provider", description = "Get all field definitions for a specific provider")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Field definitions retrieved successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid provider")
    })
    @GetMapping("/provider")
    public ResponseEntity<Response> getByProvider(@Parameter(description = "Provider type", required = true) @RequestParam Provider provider,
                                                  @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        return Response.getResponse(fieldDefinitionService.getByProvider(provider));
    }

    @Operation(summary = "Get field definition by type name", description = "Retrieve a field definition by its type name")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Field definition found"),
            @ApiResponse(responseCode = "404", description = "Field definition not found")
    })
    @GetMapping("/type/{typeName}")
    public ResponseEntity<Response> getByTypeName(@Parameter(description = "Type name") @PathVariable String typeName,
                                                  @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        return Response.getResponse(fieldDefinitionService.getByTypeName(typeName));
    }

    @Operation(summary = "Get active field definitions", description = "Get all active field definitions")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Active field definitions retrieved successfully")
    })
    @GetMapping("/active")
    public ResponseEntity<Response> getAllActive(@RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(fieldDefinitionService.getActiveFieldDefinitions());
    }

    @Operation(summary = "Update field definition", description = "Update an existing field definition")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Field definition updated successfully"),
            @ApiResponse(responseCode = "404", description = "Field definition not found")
    })
    @PutMapping("/{resourceTypeId}")
    public ResponseEntity<Response> update(@Parameter(description = "Field definition ID") @PathVariable String resourceTypeId,
                                           @RequestBody FieldDefinition request,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(resourceTypeId).build();
        return Response.updateResponse(fieldDefinitionService.updateFieldDefinition(request, identifier));
    }

    @Operation(summary = "Delete field definition", description = "Delete a field definition")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Field definition deleted successfully"),
            @ApiResponse(responseCode = "404", description = "Field definition not found")
    })
    @DeleteMapping("/{resourceTypeId}")
    public ResponseEntity<Response> delete(@Parameter(description = "Field definition ID") @PathVariable String resourceTypeId,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(resourceTypeId).build();
        return Response.deleteResponse(fieldDefinitionService.deleteFieldDefinition(identifier));
    }

    @Operation(summary = "Update field definition status", description = "Activate or deactivate a field definition")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Status updated successfully"),
            @ApiResponse(responseCode = "404", description = "Field definition not found")
    })
    @PatchMapping("/{resourceTypeId}/status")
    public ResponseEntity<Response> updateStatus(@Parameter(description = "Field definition ID") @PathVariable String resourceTypeId,
                                                 @Parameter(description = "Active status") @RequestParam Boolean isActive,
                                                 @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(resourceTypeId).build();
        return Response.updateResponse(fieldDefinitionService.updateFieldDefinitionStatus(identifier, isActive));
    }

    @Operation(summary = "Bulk reorder field definitions", description = "Reorder multiple field definitions")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Field definitions reordered successfully")
    })
    @PostMapping("/reorder")
    public ResponseEntity<Response> bulkReorder(@RequestBody List<FieldDefinition> fieldDefinitions,
                                                @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.updateResponse(fieldDefinitionService.bulkReorderFieldDefinitions(fieldDefinitions, identifier));
    }


}