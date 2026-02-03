package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.client.SchedulerRestClient;

import com.dataflow.dataloaders.services.DataloadersService;
import com.dataflow.dataloaders.util.Identifier;
import com.dataflow.dataloaders.util.Response;
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
public class DataloaderController {


    @Autowired
    private DataloadersService dataloadersService;

    @GetMapping("/running/{applicationId}")
    public ResponseEntity<Response> getAllRunningJobs(@PathVariable String applicationId,
                                                      @RequestHeader HttpHeaders headers) {
        log.info("GET /api/v1/dataloader/running");
        Identifier identifier = new Identifier(applicationId);
        List<Map<String, Object>> runningJobs =  dataloadersService.getAllRunningJobs(identifier);
        return Response.getResponse(runningJobs);
    }

    @GetMapping("/history/{applicationId}")
    public ResponseEntity<Response> getJobhistoryBydate(@PathVariable String applicationId,
                                                      @RequestParam String date,
                                                      @RequestHeader HttpHeaders headers) {
        log.info("GET /api/v1/dataloader/running");
        Identifier identifier = new Identifier(applicationId);
        List<Map<String, Object>> runningJobs =  dataloadersService.gethistorybydate(identifier,date);
        return Response.getResponse(runningJobs);
    }

    @GetMapping("/history/{applicationId}/range")
    public ResponseEntity<Response> getJobHistoryByDateRange(@PathVariable String applicationId,
                                                           @RequestParam String startDate,
                                                           @RequestParam String endDate,
                                                           @RequestHeader HttpHeaders headers) {
        log.info("GET /api/v1/dataloader/history/{}/range?startDate={}&endDate={}", applicationId, startDate, endDate);
        Identifier identifier = new Identifier(applicationId);
        List<Map<String, Object>> historyJobs = dataloadersService.getHistoryByDateRange(identifier, startDate, endDate);
        return Response.getResponse(historyJobs);
    }

    @GetMapping("/statstab/{applicationId}")
    public ResponseEntity<Response> getstats(@PathVariable String applicationId, 
                                           @RequestParam(required = false) String startDate,
                                           @RequestParam(required = false) String endDate,
                                           @RequestHeader HttpHeaders headers) {
        log.info("GET /api/v1/dataloader/statstab/{}", applicationId);
        Identifier identifier = new Identifier(applicationId);
        Object stats = dataloadersService.getstats(identifier, startDate, endDate);
        return Response.getResponse(stats);
    }
    @GetMapping("/run/{jobId}")
    public ResponseEntity<Response> runJob(@PathVariable String jobId,
                                             @RequestHeader HttpHeaders headers) {
        log.info("GET /api/v1/dataloader/statstab/{}", jobId);
        Identifier identifier = new Identifier();
        identifier.setWord(jobId);
        Object stats = dataloadersService.runJobManually(identifier);
        return Response.getResponse(stats);
    }
    @GetMapping("/status/{runningId}")
    public ResponseEntity<Response> runJob(@PathVariable Long runningId,
                                           @RequestHeader HttpHeaders headers) {
        log.info("GET /api/v1/dataloader/statstab/{}", runningId);
        Identifier identifier = new Identifier();
        identifier.setId(runningId);
        Object stats = dataloadersService.getRunningStatus(identifier);
        return Response.getResponse(stats);
    }
    @DeleteMapping("/remove/{jobId}")
    public ResponseEntity<Response> RemoveJob(@PathVariable Long jobId,
                                           @RequestHeader HttpHeaders headers) {
        log.info("POST /remove/{}", jobId);
        Identifier identifier = new Identifier();
        identifier.setId(jobId);
        Object stats = dataloadersService.removeSheduleJob(identifier);
        return Response.getResponse(stats);
    }

}