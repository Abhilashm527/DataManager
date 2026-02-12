package com.dataflow.dataloaders.services;

import com.dataflow.dataloaders.dao.SystemSettingDao;
import com.dataflow.dataloaders.entity.SystemSetting;
import com.dataflow.dataloaders.util.DateUtils;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
@Service
public class SystemSettingService {

    @Autowired
    private SystemSettingDao systemSettingDao;

    private static final String PRIMARY_TIMEZONE_KEY = "primary_timezone";
    private static final String PRIMARY_DATE_PATTERN_KEY = "primary_date_pattern";
    private String cachedTimezone = "UTC";
    private String cachedDatePattern = "dd-MM-yyyy HH:mm:ss";

    @PostConstruct
    public void init() {
        refreshCache();
    }

    public void refreshCache() {
        Optional<SystemSetting> tzSetting = systemSettingDao.getByKey(PRIMARY_TIMEZONE_KEY);
        if (tzSetting.isPresent()) {
            cachedTimezone = tzSetting.get().getValue();
            DateUtils.setPrimaryZoneId(getPrimaryZoneId()); // Synchronize with DateUtils
        }

        Optional<SystemSetting> patSetting = systemSettingDao.getByKey(PRIMARY_DATE_PATTERN_KEY);
        if (patSetting.isPresent()) {
            cachedDatePattern = patSetting.get().getValue();
            DateUtils.setPrimaryPattern(cachedDatePattern); // Synchronize with DateUtils
        }

        log.info("System settings loaded - Timezone: {}, Pattern: {}", cachedTimezone, cachedDatePattern);
    }

    public String getPrimaryTimezone() {
        return cachedTimezone;
    }

    public String getPrimaryDatePattern() {
        return cachedDatePattern;
    }

    public ZoneId getPrimaryZoneId() {
        try {
            return ZoneId.of(cachedTimezone);
        } catch (Exception e) {
            log.error("Invalid timezone in settings: {}. Falling back to UTC.", cachedTimezone);
            return ZoneId.of("UTC");
        }
    }

    public boolean updatePrimaryTimezone(String timezone) {
        try {
            ZoneId zoneId = ZoneId.of(timezone); // Validate timezone
            SystemSetting setting = SystemSetting.builder()
                    .key(PRIMARY_TIMEZONE_KEY)
                    .value(timezone)
                    .updatedBy("admin")
                    .build();
            int rows = systemSettingDao.update(setting);
            if (rows > 0) {
                cachedTimezone = timezone;
                DateUtils.setPrimaryZoneId(zoneId); // Synchronize with DateUtils
                return true;
            }
        } catch (Exception e) {
            log.error("Error updating timezone: {}", e.getMessage());
        }
        return false;
    }

    public boolean updatePrimaryDatePattern(String pattern) {
        try {
            java.time.format.DateTimeFormatter.ofPattern(pattern); // Validate pattern
            SystemSetting setting = SystemSetting.builder()
                    .key(PRIMARY_DATE_PATTERN_KEY)
                    .value(pattern)
                    .updatedBy("admin")
                    .build();
            int rows = systemSettingDao.update(setting);
            if (rows > 0) {
                cachedDatePattern = pattern;
                DateUtils.setPrimaryPattern(pattern); // Synchronize with DateUtils
                return true;
            }
        } catch (Exception e) {
            log.error("Error updating date pattern: {}", e.getMessage());
        }
        return false;
    }

    public List<String> getAllAvailableTimezones() {
        List<String> zones = new ArrayList<>(ZoneId.getAvailableZoneIds());
        Collections.sort(zones);
        return zones;
    }

    public List<java.util.Map<String, String>> getSuggestedDatePatterns() {
        List<String> patterns = List.of(
                "dd-MM-yyyy HH:mm:ss",
                "yyyy-MM-dd HH:mm:ss",
                "dd/MM/yyyy HH:mm:ss",
                "dd-MM-yyyy hh:mm:ss a",
                "MM/dd/yyyy hh:mm:ss a",
                "dd MMM yyyy hh:mm:ss a",
                "dd MMMM yyyy HH:mm",
                "EEEE, dd MMM yyyy HH:mm");

        java.time.ZonedDateTime now = java.time.ZonedDateTime.now(getPrimaryZoneId());
        List<java.util.Map<String, String>> suggestions = new java.util.ArrayList<>();

        for (String pattern : patterns) {
            java.util.Map<String, String> item = new java.util.HashMap<>();
            item.put("pattern", pattern);
            try {
                String example = now
                        .format(java.time.format.DateTimeFormatter.ofPattern(pattern, java.util.Locale.ENGLISH))
                        .toUpperCase();
                item.put("example", example);
            } catch (Exception e) {
                item.put("example", "Invalid pattern");
            }
            suggestions.add(item);
        }
        return suggestions;
    }
}
