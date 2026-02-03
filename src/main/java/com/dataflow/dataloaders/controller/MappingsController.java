package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.entity.Mappings;
import com.dataflow.dataloaders.services.MappingsService;
import com.dataflow.dataloaders.util.Identifier;
import com.dataflow.dataloaders.util.Response;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import static com.dataflow.dataloaders.config.APIConstants.MAPPINGS_BASE_PATH;

@Slf4j
@RestController
@RequestMapping(MAPPINGS_BASE_PATH)
public class MappingsController {

    protected static final String logPrefix = "{} : {}";

    @Autowired
    private MappingsService mappingsService;

    /**
     * Create a new mapping
     */
    @PostMapping("/{mappingId}")
    public ResponseEntity<Response> create(@RequestBody Mappings mappings,
                                           @PathVariable String mappingId,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        mappings.setItemId(mappingId);
        return Response.createResponse(mappingsService.create(mappings, identifier));
    }

    /**
     * Get a mapping by ID
     */
    @GetMapping("/{mappingId}")
    public ResponseEntity<Response> get(@PathVariable String mappingId,
                                        @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(mappingId).build();
        return Response.getResponse(mappingsService.getMapping(identifier));
    }

    /**
     * Get all mappings
     */
    @GetMapping
    public ResponseEntity<Response> getAll(@RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(mappingsService.getAllMappings(identifier));
    }

    /**
     * Get mappings by item_id
     */
    @GetMapping("/item/{itemId}")
    public ResponseEntity<Response> getByItemId(@PathVariable String itemId,
                                                @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(itemId).build();
        return Response.getResponse(mappingsService.getMappingsByItemId(identifier));
    }

    /**
     * Update a mapping
     */
    @PutMapping("/{mappingId}")
    public ResponseEntity<Response> update(@PathVariable String mappingId,
                                           @RequestBody Mappings request,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(mappingId).build();
        return Response.updateResponse(mappingsService.updateMapping(request, identifier));
    }

    /**
     * Delete a mapping
     */
    @DeleteMapping("/{mappingId}")
    public ResponseEntity<Response> delete(@PathVariable String mappingId,
                                           @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(mappingId).build();
        return Response.deleteResponse(mappingsService.deleteMapping(identifier));
    }

    /**
     * Activate/Deactivate a mapping
     */
    @PatchMapping("/{mappingId}/status")
    public ResponseEntity<Response> updateStatus(@PathVariable String mappingId,
                                                 @RequestParam Boolean isActive,
                                                 @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(mappingId).build();
        return Response.updateResponse(mappingsService.updateMappingStatus(identifier, isActive));
    }

    /**
     * Validate mapping configuration
     */
    @PostMapping("/validate")
    public ResponseEntity<Response> validateMapping(@RequestBody Mappings mappings,
                                                    @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.getResponse(mappingsService.validateMapping(mappings, identifier));
    }
}