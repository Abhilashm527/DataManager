package com.dataflow.dataloaders.client;

import com.dataflow.dataloaders.dto.JobScheduledResponse;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.Identifier;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.client.RestTemplate;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class SchedulerRestClient {

    private final RestTemplate restTemplate;
    
    @Value("${scheduler.service.url:http://localhost:8881/restservices/ivoyant/v1/dataloaders}")
    private String schedulerServiceUrl;
    @Autowired
    private ObjectMapper objectMapper;

    public SchedulerRestClient() {
        this.restTemplate = new RestTemplate();
    }

    public JobScheduledResponse scheduleJob(Map<String, Object> jobConfig) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<Map<String, Object>> request = new HttpEntity<>(jobConfig, headers);
            ResponseEntity<String> response = restTemplate.postForEntity(
                schedulerServiceUrl + "/schedule/config/add",
                request, 
                String.class
            );
            JobScheduledResponse jobScheduledResponse = objectMapper.readValue(response.getBody(), JobScheduledResponse.class);
            
            log.info("Job scheduled successfully: {}", jobScheduledResponse);
            return jobScheduledResponse;
            
        } catch (Exception e) {
            log.error("Failed to schedule job: {}", e.getMessage());
            throw new RuntimeException("Failed to schedule job", e);
        }
    }

    public List<Map<String, Object>> getAllRunningJobs() {
        try {
            ResponseEntity<String> response = restTemplate.exchange(
                schedulerServiceUrl + "/schedule/view",
                HttpMethod.GET,
                null,
                String.class
            );
            
            log.info("Retrieved running jobs successfully");
            TypeReference<List<Map<String, Object>>> typeRef = new TypeReference<List<Map<String, Object>>>() {};
            List<Map<String, Object>> runningJobs = objectMapper.readValue(response.getBody(), typeRef);
            return runningJobs;
        } catch (Exception e) {
            log.error("Failed to get running jobs: {}", e.getMessage());
            return List.of();
        }
    }

    public List<Map<String, Object>> getAllHistoryByDate(String date) {
        try {
            ResponseEntity<String> response = restTemplate.exchange(
                schedulerServiceUrl + "/schedule/history?date=" + date,
                HttpMethod.GET,
                null,
                String.class
            );
            
            log.info("Retrieved job history for date: {}", date);
            TypeReference<List<Map<String, Object>>> typeRef = new TypeReference<List<Map<String, Object>>>() {};
            return objectMapper.readValue(response.getBody(), typeRef);
        } catch (Exception e) {
            log.error("Failed to get job history: {}", e.getMessage());
            return List.of();
        }
    }

    public List<Map<String, Object>> getHistoryByDateRange(String startDate, String endDate) {
        try {
            ResponseEntity<String> response = restTemplate.exchange(
                schedulerServiceUrl + "/schedule/history/range?start=" + startDate + "&end=" + endDate,
                HttpMethod.GET,
                null,
                String.class
            );
            
            log.info("Retrieved job history from {} to {}", startDate, endDate);
            TypeReference<List<Map<String, Object>>> typeRef = new TypeReference<List<Map<String, Object>>>() {};
            return objectMapper.readValue(response.getBody(), typeRef);
        } catch (Exception e) {
            log.error("Failed to get job history: {}", e.getMessage());
            return List.of();
        }
    }

    public Object runJobManually(Map<String, Object> body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(body, headers);
        try {
            ResponseEntity<Long> response = restTemplate.exchange(
                    schedulerServiceUrl + "/job/start/jobconfig",
                    HttpMethod.POST,
                    request,
                    Long.class
            );

            log.info("job started sucesfully, Running Id: {} ", response);
            Map<String,Long> data = new LinkedHashMap<>();
            data.put("jobId", response.getBody());
            return data;
        } catch (Exception e) {
            log.error("Failed to get job history: {}", e.getMessage());
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, "Failed to run job: "+e.getMessage());
        }
    }

    public Object getRunningStats(Identifier identifier) {
        try {
            ResponseEntity<String> response = restTemplate.exchange(
                    schedulerServiceUrl + "/status/"+ identifier.getId(),
                    HttpMethod.GET,
                    null,
                    String.class
            );
            log.info("job started sucesfully, Running Id: {} ", response);
            TypeReference<Map<String, Object>> typeRef = new TypeReference<Map<String, Object>>() {};
            return objectMapper.readValue(response.getBody(), typeRef);
        } catch (Exception e) {
            log.error("Failed to get job history: {}", e.getMessage());
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, "Failed to run job: "+e.getMessage());
        }
    }

    public Object removeFromSchedule(Identifier identifier) {
        try {
            ResponseEntity<String> response = restTemplate.exchange(
                    schedulerServiceUrl + "/schedule/delete/"+ identifier.getId(),
                    HttpMethod.POST,
                    null,
                    String.class
            );
            log.info("job started sucesfully, Running Id: {} ", response);
            TypeReference<Map<String, Object>> typeRef = new TypeReference<Map<String, Object>>() {};
            return objectMapper.readValue(response.getBody(), typeRef);
        } catch (Exception e) {
            log.error("Failed to get job history: {}", e.getMessage());
            throw new DataloadersException(ErrorFactory.DATABASE_EXCEPTION, "Failed to run job: "+e.getMessage());
        }
    }
}