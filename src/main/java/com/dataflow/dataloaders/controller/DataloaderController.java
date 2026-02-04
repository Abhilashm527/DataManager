package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.client.SchedulerRestClient;

import com.dataflow.dataloaders.services.DataloadersService;
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
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/v1/dataloader")
@Tag(name = "Dataloader", description = "Dataloader job management APIs")
public class DataloaderController {


    @Autowired
    private DataloadersService dataloadersService;

    @Operation(summary = "Get running jobs", description = "Get all running jobs for an application")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Running jobs retrieved successfully")
    })
    @GetMapping("/running/{applicationId}")
    public ResponseEntity<Response> getAllRunningJobs(@Parameter(description = "Application ID") @PathVariable String applicationId,
                                                      @RequestHeader HttpHeaders headers) {
        log.info("GET /api/v1/dataloader/running");
        Identifier identifier = new Identifier(applicationId);
        List<Map<String, Object>> runningJobs =  dataloadersService.getAllRunningJobs(identifier);
        return Response.getResponse(runningJobs);
    }

    @Operation(summary = "Get job history by date", description = "Get job history for a specific date")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Job history retrieved successfully")
    })
    @GetMapping("/history/{applicationId}")
    public ResponseEntity<Response> getJobhistoryBydate(@Parameter(description = "Application ID") @PathVariable String applicationId,
                                                      @Parameter(description = "Date") @RequestParam String date,
                                                      @RequestHeader HttpHeaders headers) {
        log.info("GET /api/v1/dataloader/running");
        Identifier identifier = new Identifier(applicationId);
        List<Map<String, Object>> runningJobs =  dataloadersService.gethistorybydate(identifier,date);
        return Response.getResponse(runningJobs);
    }

    @Operation(summary = "Get job history by date range", description = "Get job history for a date range")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Job history retrieved successfully")
    })
    @GetMapping("/history/{applicationId}/range")
    public ResponseEntity<Response> getJobHistoryByDateRange(@Parameter(description = "Application ID") @PathVariable String applicationId,
                                                           @Parameter(description = "Start date") @RequestParam String startDate,
                                                           @Parameter(description = "End date") @RequestParam String endDate,
                                                           @RequestHeader HttpHeaders headers) {
        log.info("GET /api/v1/dataloader/history/{}/range?startDate={}&endDate={}", applicationId, startDate, endDate);
        Identifier identifier = new Identifier(applicationId);
        List<Map<String, Object>> historyJobs = dataloadersService.getHistoryByDateRange(identifier, startDate, endDate);
        return Response.getResponse(historyJobs);
    }

    @Operation(summary = "Get statistics", description = "Get statistics for an application")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Statistics retrieved successfully")
    })
    @GetMapping("/statstab/{applicationId}")
    public ResponseEntity<Response> getstats(@Parameter(description = "Application ID") @PathVariable String applicationId, 
                                           @Parameter(description = "Start date") @RequestParam(required = false) String startDate,
                                           @Parameter(description = "End date") @RequestParam(required = false) String endDate,
                                           @RequestHeader HttpHeaders headers) {
        log.info("GET /api/v1/dataloader/statstab/{}", applicationId);
        Identifier identifier = new Identifier(applicationId);
        Object stats = dataloadersService.getstats(identifier, startDate, endDate);
        return Response.getResponse(stats);
    }
    @Operation(summary = "Run job manually", description = "Manually trigger a job")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Job triggered successfully")
    })
    @GetMapping("/run/{jobId}")
    public ResponseEntity<Response> runJob(@Parameter(description = "Job ID") @PathVariable String jobId,
                                             @RequestHeader HttpHeaders headers) {
        log.info("GET /api/v1/dataloader/statstab/{}", jobId);
        Identifier identifier = new Identifier();
        identifier.setWord(jobId);
        Object stats = dataloadersService.runJobManually(identifier);
        return Response.getResponse(stats);
    }
    @Operation(summary = "Get job status", description = "Get status of a running job")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Job status retrieved successfully")
    })
    @GetMapping("/status/{runningId}")
    public ResponseEntity<Response> getJobStatus(@Parameter(description = "Running job ID") @PathVariable Long runningId,
                                           @RequestHeader HttpHeaders headers) {
        log.info("GET /api/v1/dataloader/statstab/{}", runningId);
        Identifier identifier = new Identifier();
        identifier.setId(runningId);
        Object stats = dataloadersService.getRunningStatus(identifier);
        return Response.getResponse(stats);
    }
    @Operation(summary = "Remove scheduled job", description = "Remove a scheduled job")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Job removed successfully")
    })
    @DeleteMapping("/remove/{jobId}")
    public ResponseEntity<Response> RemoveJob(@Parameter(description = "Job ID") @PathVariable Long jobId,
                                           @RequestHeader HttpHeaders headers) {
        log.info("POST /remove/{}", jobId);
        Identifier identifier = new Identifier();
        identifier.setId(jobId);
        Object stats = dataloadersService.removeSheduleJob(identifier);
        return Response.getResponse(stats);
    }

}