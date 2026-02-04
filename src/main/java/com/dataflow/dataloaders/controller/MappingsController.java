package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.entity.Mappings;
import com.dataflow.dataloaders.services.MappingsService;
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

import static com.dataflow.dataloaders.config.APIConstants.MAPPINGS_BASE_PATH;

@Slf4j
@RestController
@RequestMapping(MAPPINGS_BASE_PATH)
@Tag(name = "Mappings", description = "Data mapping management APIs")
public class MappingsController {

    protected static final String logPrefix = "{} : {}";

    @Autowired
    private MappingsService mappingsService;

    @Operation(summary = "Create mapping", description = "Create a new data mapping")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "201", description = "Mapping created successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid input")
    })
    @PostMapping("/{mappingId}")
    public ResponseEntity<Response> create(@RequestBody Mappings mappings,
                                           @Parameter(description = "Mapping ID") @PathVariable String mappingId,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        mappings.setItemId(mappingId);
        return Response.createResponse(mappingsService.create(mappings, identifier));
    }

    @Operation(summary = "Get mapping by ID", description = "Get mapping by ID")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Mapping found"),
            @ApiResponse(responseCode = "404", description = "Mapping not found")
    })
    @GetMapping("/{mappingId}")
    public ResponseEntity<Response> get(@Parameter(description = "Mapping ID") @PathVariable String mappingId,
                                        @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(mappingId).build();
        return Response.getResponse(mappingsService.getMapping(identifier));
    }

    @Operation(summary = "Get all mappings", description = "Get all data mappings")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Mappings retrieved")
    })
    @GetMapping
    public ResponseEntity<Response> getAll(@RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(mappingsService.getAllMappings(identifier));
    }

    @Operation(summary = "Get mappings by item", description = "Get mappings by item ID")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Mappings retrieved")
    })
    @GetMapping("/item/{itemId}")
    public ResponseEntity<Response> getByItemId(@Parameter(description = "Item ID") @PathVariable String itemId,
                                                @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(itemId).build();
        return Response.getResponse(mappingsService.getMappingsByItemId(identifier));
    }

    @Operation(summary = "Update mapping", description = "Update an existing mapping")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Mapping updated successfully"),
            @ApiResponse(responseCode = "404", description = "Mapping not found")
    })
    @PutMapping("/{mappingId}")
    public ResponseEntity<Response> update(@Parameter(description = "Mapping ID") @PathVariable String mappingId,
                                           @RequestBody Mappings request,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(mappingId).build();
        return Response.updateResponse(mappingsService.updateMapping(request, identifier));
    }

    @Operation(summary = "Delete mapping", description = "Delete a mapping")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Mapping deleted successfully"),
            @ApiResponse(responseCode = "404", description = "Mapping not found")
    })
    @DeleteMapping("/{mappingId}")
    public ResponseEntity<Response> delete(@Parameter(description = "Mapping ID") @PathVariable String mappingId,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(mappingId).build();
        return Response.deleteResponse(mappingsService.deleteMapping(identifier));
    }

    @Operation(summary = "Update mapping status", description = "Activate or deactivate a mapping")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Status updated successfully")
    })
    @PatchMapping("/{mappingId}/status")
    public ResponseEntity<Response> updateStatus(@Parameter(description = "Mapping ID") @PathVariable String mappingId,
                                                 @Parameter(description = "Active status") @RequestParam Boolean isActive,
                                                 @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(mappingId).build();
        return Response.updateResponse(mappingsService.updateMappingStatus(identifier, isActive));
    }

    @Operation(summary = "Validate mapping", description = "Validate mapping configuration")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Mapping validation result")
    })
    @PostMapping("/validate")
    public ResponseEntity<Response> validateMapping(@RequestBody Mappings mappings,
                                                    @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(mappingsService.validateMapping(mappings, identifier));
    }
}