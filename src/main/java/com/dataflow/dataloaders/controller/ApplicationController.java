package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.entity.Application;
import com.dataflow.dataloaders.services.ApplicationService;
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

import static com.dataflow.dataloaders.config.APIConstants.APPLICATION_BASE_PATH;

@Slf4j
@RestController
@RequestMapping(APPLICATION_BASE_PATH)
@Tag(name = "Applications", description = "Application management APIs")
public class ApplicationController {

        protected static final String logPrefix = "{} : {}";

        @Autowired
        private ApplicationService applicationService;

        @Operation(summary = "Create application", description = "Create a new application")
        @ApiResponses(value = {
                        @ApiResponse(responseCode = "201", description = "Application created successfully"),
                        @ApiResponse(responseCode = "400", description = "Invalid input")
        })
        @PostMapping
        public ResponseEntity<Response> create(@RequestBody Application application,
                        @RequestHeader HttpHeaders headers) {
                log.info(logPrefix, this.getClass().getSimpleName(),
                                Thread.currentThread().getStackTrace()[1].getMethodName());
                Identifier identifier = Identifier.builder().headers(headers).build();
                return Response.createResponse(applicationService.create(application, identifier));
        }

        @Operation(summary = "Get application by ID", description = "Get application details by ID")
        @ApiResponses(value = {
                        @ApiResponse(responseCode = "200", description = "Application found"),
                        @ApiResponse(responseCode = "404", description = "Application not found")
        })
        @GetMapping("/{applicationId}")
        public ResponseEntity<Response> get(
                        @Parameter(description = "Application ID") @PathVariable String applicationId,
                        @RequestHeader HttpHeaders headers) {
                log.info(logPrefix, this.getClass().getSimpleName(),
                                Thread.currentThread().getStackTrace()[1].getMethodName());
                Identifier identifier = Identifier.builder().headers(headers).word(applicationId).build();
                return Response.getResponse(applicationService.getApplication(identifier));
        }

        @Operation(summary = "Get all applications", description = "Get all applications")
        @ApiResponses(value = {
                        @ApiResponse(responseCode = "200", description = "Applications retrieved")
        })
        @GetMapping()
        public ResponseEntity<Response> getAll(@RequestParam(required = false) String search,
                        @RequestHeader HttpHeaders headers) {
                log.info(logPrefix, this.getClass().getSimpleName(),
                                Thread.currentThread().getStackTrace()[1].getMethodName());
                Identifier identifier = Identifier.builder().headers(headers).build();
                return Response.getResponse(applicationService.getAllApplications(identifier, search));
        }

        @Operation(summary = "Update application", description = "Update an existing application")
        @ApiResponses(value = {
                        @ApiResponse(responseCode = "200", description = "Application updated successfully"),
                        @ApiResponse(responseCode = "404", description = "Application not found")
        })
        @PutMapping("/{applicationId}")
        public ResponseEntity<Response> update(
                        @Parameter(description = "Application ID") @PathVariable String applicationId,
                        @RequestBody Application application,
                        @RequestHeader HttpHeaders headers) {
                log.info(logPrefix, this.getClass().getSimpleName(),
                                Thread.currentThread().getStackTrace()[1].getMethodName());
                Identifier identifier = Identifier.builder().headers(headers).word(applicationId).build();
                return Response.updateResponse(applicationService.updateApplication(application, identifier));
        }

        @Operation(summary = "Delete application", description = "Delete an application")
        @ApiResponses(value = {
                        @ApiResponse(responseCode = "200", description = "Application deleted successfully"),
                        @ApiResponse(responseCode = "404", description = "Application not found")
        })
        @DeleteMapping("/{applicationId}")
        public ResponseEntity<Response> delete(
                        @Parameter(description = "Application ID") @PathVariable String applicationId,
                        @RequestHeader HttpHeaders headers) {
                log.info(logPrefix, this.getClass().getSimpleName(),
                                Thread.currentThread().getStackTrace()[1].getMethodName());
                Identifier identifier = Identifier.builder().headers(headers).word(applicationId).build();
                return Response.deleteResponse(applicationService.deleteApplication(identifier));
        }
}