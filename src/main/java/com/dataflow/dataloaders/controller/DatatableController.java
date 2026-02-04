package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.entity.Datatable;
import com.dataflow.dataloaders.entity.Item;
import com.dataflow.dataloaders.services.DatatableService;
import com.dataflow.dataloaders.services.ItemService;
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

@Slf4j
@RestController
@RequestMapping("/api/v1/dataloader")
@Tag(name = "Datatable", description = "Datatable management APIs")
public class DatatableController {
    protected static final String logPrefix = "{} : {}";

    @Autowired
    private DatatableService datatableService;

    @Operation(summary = "Create datatable", description = "Create a new datatable")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "201", description = "Datatable created successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid input")
    })
    @PostMapping("/datatable")
    public ResponseEntity<Response> create(@RequestBody Datatable datatable, @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).build();
        return Response.createResponse(datatableService.create(datatable, identifier));
    }

    @Operation(summary = "Get datatables by application", description = "Get all datatables for an application")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Datatables retrieved successfully")
    })
    @GetMapping("/datatable/{applicationId}")
    public ResponseEntity<Response> getAll(@Parameter(description = "Application ID") @PathVariable String applicationId, @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(applicationId).build();
        return Response.createResponse(datatableService.get(identifier));
    }
    
    @Operation(summary = "Delete datatable", description = "Delete a datatable by ID")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Datatable deleted successfully"),
            @ApiResponse(responseCode = "404", description = "Datatable not found")
    })
    @DeleteMapping("/datatable/{datatableId}")
    public ResponseEntity<Response> deleteById(@Parameter(description = "Datatable ID") @PathVariable String datatableId, @RequestHeader HttpHeaders headers) {
        log.info(logPrefix, this.getClass().getSimpleName(), Thread.currentThread().getStackTrace()[1].getMethodName());
        Identifier identifier = Identifier.builder().headers(headers).word(datatableId).build();
        return Response.createResponse(datatableService.delete(identifier));
    }
}
