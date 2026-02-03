package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.client.SchedulerRestClient;
import com.dataflow.dataloaders.dao.DeployDao;
import com.dataflow.dataloaders.dto.StatsResponse;
import com.dataflow.dataloaders.entity.Deploy;
import com.dataflow.dataloaders.exception.DataloadersException;
import com.dataflow.dataloaders.exception.ErrorFactory;
import com.dataflow.dataloaders.util.Identifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Service
public class DataloadersService {

    @Autowired
    private SchedulerRestClient schedulerRestClient;
    @Autowired
    private DeployService deployService;
    @Autowired
    private DeployDao deployDao;
    
    public List<Map<String, Object>> getAllRunningJobs(Identifier identifier) {
        Set<Long> scheduleIds = deployService.getAllDeploysByAppId(identifier)
                .stream()
                .filter(deploy -> !deploy.isManualRun())
                .map(Deploy::getSchedulerId)
                .filter(id -> id != null)
                .collect(Collectors.toSet());
        
        return schedulerRestClient.getAllRunningJobs()
                .stream()
                .filter(job -> {
                    try {
                        Object jobIdObj = job.get("id");
                        if (jobIdObj == null) return false;
                        
                        Long jobId;
                        if (jobIdObj instanceof String) {
                            jobId = Long.valueOf((String) jobIdObj);
                        } else if (jobIdObj instanceof Integer) {
                            jobId = ((Integer) jobIdObj).longValue();
                        } else if (jobIdObj instanceof Long) {
                            jobId = (Long) jobIdObj;
                        } else {
                            return false;
                        }
                        
                        return scheduleIds.contains(jobId);
                    } catch (NumberFormatException e) {
                        return false;
                    }
                })
                .collect(Collectors.toList());
    }

    public List<Map<String, Object>> gethistorybydate(Identifier identifier, String date) {
        Set<Long> scheduleIds = deployService.getAllDeploysByAppId(identifier)
                .stream()
                .filter(deploy -> !deploy.isManualRun())
                .map(Deploy::getSchedulerId)
                .filter(id -> id != null)
                .collect(Collectors.toSet());
        return schedulerRestClient.getAllHistoryByDate(date)
                .stream()
                .filter(job -> {
                    try {
                        Object jobIdObj = job.get("schedulerId");
                        if (jobIdObj == null) return false;

                        Long jobId;
                        if (jobIdObj instanceof String) {
                            jobId = Long.valueOf((String) jobIdObj);
                        } else if (jobIdObj instanceof Integer) {
                            jobId = ((Integer) jobIdObj).longValue();
                        } else if (jobIdObj instanceof Long) {
                            jobId = (Long) jobIdObj;
                        } else {
                            return false;
                        }

                        return scheduleIds.contains(jobId);
                    } catch (NumberFormatException e) {
                        return false;
                    }
                })
                .collect(Collectors.toList());
    }

    public List<Map<String, Object>> getHistoryByDateRange(Identifier identifier, String startDate, String endDate) {
        Set<Long> scheduleIds = deployService.getAllDeploysByAppId(identifier)
                .stream()
                .filter(deploy -> !deploy.isManualRun())
                .map(Deploy::getSchedulerId)
                .filter(id -> id != null)
                .collect(Collectors.toSet());
        
        return schedulerRestClient.getHistoryByDateRange(startDate, endDate)
                .stream()
                .filter(job -> {
                    try {
                        Object jobIdObj = job.get("schedulerId");
                        if (jobIdObj == null) return false;

                        Long jobId;
                        if (jobIdObj instanceof String) {
                            jobId = Long.valueOf((String) jobIdObj);
                        } else if (jobIdObj instanceof Integer) {
                            jobId = ((Integer) jobIdObj).longValue();
                        } else if (jobIdObj instanceof Long) {
                            jobId = (Long) jobIdObj;
                        } else {
                            return false;
                        }

                        return scheduleIds.contains(jobId);
                    } catch (NumberFormatException e) {
                        return false;
                    }
                })
                .collect(Collectors.toList());
    }


    public StatsResponse getstats(Identifier identifier, String startDate, String endDate) {
        List<Deploy> deploys = deployService.getAllDeploysByAppId(identifier);
        Map<String, Object> stats = new java.util.HashMap<>();
        int activeJobs=0 , inactiveJobs=0 , manualJobs=0 , scheduledJobs=0;
        
        for(Deploy each: deploys){
            if(each.isManualRun())
                manualJobs++;
            else
                scheduledJobs++;
            if(each.isActive())
                activeJobs++;
            else
                inactiveJobs++;
        }

        if(startDate == null || endDate == null){
            startDate = java.time.LocalDate.now().minusDays(1).toString();
            endDate = java.time.LocalDate.now().toString();
        }
        
        // Get history for specified date range
        List<Map<String, Object>> historyJobs = getHistoryByDateRange(identifier, startDate, endDate);
        
        int successCount = 0;
        int failureCount = 0;
        int stoppedCount = 0;
        for (Map<String, Object> job : historyJobs) {
            Object exitCode = job.get("exitCode");
            if (exitCode != null) {
                if (exitCode.equals("COMPLETED")) {
                    successCount++;
                } else if (exitCode.equals("FAILED") ){
                    failureCount++;
                } else if(exitCode.equals("STOPPED")){
                    stoppedCount++;
                }
            }
        }
        
        double successRate = (successCount + failureCount) > 0 ? 
            (double) successCount / (successCount + failureCount) * 100 : 0.0;

        StatsResponse statsResponse = new StatsResponse();
        statsResponse.setActiveDeployments(activeJobs);
        statsResponse.setInactiveDeployments(inactiveJobs);
        statsResponse.setManualJobs(manualJobs);
        statsResponse.setScheduledJobs(scheduledJobs);
        statsResponse.setSuccessCount(successCount);
        statsResponse.setFailureCount(failureCount);
        statsResponse.setStoppedCount(stoppedCount);
        statsResponse.setSuccessRate(Math.round(successRate * 100.0) / 100.0);
        
        return statsResponse;
    }

    public Object runJobManually(Identifier identifier) {
        Map<String, Object> deploy = deployService.getJobConfigByJobId(identifier);
        if (deploy == null) {
            throw new RuntimeException("Deploy not found");
        }
        return schedulerRestClient.runJobManually(deploy);
    }

    public Object getRunningStatus(Identifier identifier) {
        return schedulerRestClient.getRunningStats(identifier);
    }

    public Object removeSheduleJob(Identifier identifier) {
         schedulerRestClient.removeFromSchedule(identifier);
        return deployDao.deleteBySheduleId(identifier);
    }
}