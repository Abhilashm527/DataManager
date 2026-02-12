package com.dataflow.dataloaders.controller;

import com.dataflow.dataloaders.services.SystemSettingService;
import com.dataflow.dataloaders.util.Response;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/api/v1/settings")
@Tag(name = "System Settings", description = "Global system configuration APIs")
public class SystemSettingController {

    @Autowired
    private SystemSettingService systemSettingService;

    @Operation(summary = "Get all available timezones")
    @GetMapping("/timezones")
    public ResponseEntity<Response> getAllTimezones() {
        return Response.getResponse(systemSettingService.getAllAvailableTimezones());
    }

    @Operation(summary = "Get primary timezone")
    @GetMapping("/timezone/primary")
    public ResponseEntity<Response> getPrimaryTimezone() {
        return Response.getResponse(systemSettingService.getPrimaryTimezone());
    }

    @Operation(summary = "Update primary timezone")
    @PostMapping("/timezone/primary")
    public ResponseEntity<Response> updatePrimaryTimezone(@RequestParam String timezone) {
        log.info("Updating primary timezone to: {}", timezone);
        boolean updated = systemSettingService.updatePrimaryTimezone(timezone);
        if (updated) {
            return Response.getResponse("Timezone updated successfully");
        } else {
            return Response.getResponse("Failed to update timezone. Please ensure it is a valid ZoneId.");
        }
    }

    @Operation(summary = "Get suggested date patterns")
    @GetMapping("/date-patterns/suggested")
    public ResponseEntity<Response> getSuggestedDatePatterns() {
        return Response.getResponse(systemSettingService.getSuggestedDatePatterns());
    }

    @Operation(summary = "Get primary date pattern")
    @GetMapping("/date-pattern/primary")
    public ResponseEntity<Response> getPrimaryDatePattern() {
        return Response.getResponse(systemSettingService.getPrimaryDatePattern());
    }

    @Operation(summary = "Update primary date pattern")
    @PostMapping("/date-pattern/primary")
    public ResponseEntity<Response> updatePrimaryDatePattern(@RequestParam String pattern) {
        log.info("Updating primary date pattern to: {}", pattern);
        boolean updated = systemSettingService.updatePrimaryDatePattern(pattern);
        if (updated) {
            return Response.getResponse("Date pattern updated successfully");
        } else {
            return Response.getResponse(
                    "Failed to update date pattern. Please ensure it is a valid DateTimeFormatter pattern.");
        }
    }
}
