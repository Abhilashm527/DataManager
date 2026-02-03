package com.dataflow.dataloaders.util;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class DateUtils {
    public static final ZoneId ZONE_ID_SYS_DEFAULT = ZoneId.systemDefault();
    public static final ZoneId ZONE_ID_IST = ZoneId.of("Asia/Kolkata");
    public static final ZoneId ZONE_ID_UTC = ZoneId.of("UTC");
    public static final String DATE_FORMAT = "dd-MM-yyyy";

    public static LocalDateTime getLocalDateTime() {
        return LocalDateTime.now();
    }

    public static LocalDateTime getLocalDateTimeInUTC() {
        return LocalDateTime.now(ZoneOffset.UTC);
    }

    public static LocalDateTime getLocalDateTimeInCurrentZone(LocalDateTime localDateTime) {
        localDateTime = getLocalDateTimeInUTC();
        ZonedDateTime utcDateTime = localDateTime.atZone(ZONE_ID_UTC).withZoneSameInstant(ZONE_ID_SYS_DEFAULT);
        return utcDateTime.toLocalDateTime();
    }

    public static LocalDateTime getLocalDateTimeInISTZone() {
        LocalDateTime localDateTime = getLocalDateTimeInUTC();
        ZonedDateTime utcDateTime = localDateTime.atZone(ZONE_ID_UTC).withZoneSameInstant(ZONE_ID_IST);
        return utcDateTime.toLocalDateTime();
    }

    public static LocalDateTime getLocalDateTimeInISTZone(Long unixTimestamp) {
        return LocalDateTime.ofInstant(Instant.ofEpochSecond(unixTimestamp), ZONE_ID_IST);
    }

    public static Long getUnixTimestampInUTC() {
        return Instant.now().getEpochSecond();
    }

    public static Long getUnixTimestampInUTC(LocalDateTime localDateTime) {
        return localDateTime.toEpochSecond(ZoneOffset.UTC);
    }

    public static String getFormattedDate(Long epochSecond) {
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(epochSecond), ZONE_ID_UTC);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
        return dateTime.format(formatter);
    }

    public static ZonedDateTime getUnixTimestampStartDayInZonedDateTime() {
        return ZonedDateTime.now(ZoneOffset.UTC).toLocalDate().atStartOfDay(ZoneOffset.UTC);
    }

    public static Long getUnixTimestampStartDayInUTC() {
        return getUnixTimestampStartDayInZonedDateTime().toEpochSecond();
    }

    public static Long getUnixTimestampStartDayInUTC(Long days) {
        ZonedDateTime startOfIncrementalDaysUtc = getUnixTimestampStartDayInZonedDateTime().plus(days, ChronoUnit.DAYS);
        return startOfIncrementalDaysUtc.toEpochSecond();
    }

    public static Long getUnixTimestampMinusDaysInUTC(Long days) {
        return getUnixTimestampStartDayInUTC() - Duration.ofDays(days).getSeconds();
    }

    public static Long getUnixTimestampMinusDaysInUTC(Long unixTimestamp, Long days) {
        return unixTimestamp - Duration.ofDays(days).getSeconds();
    }

    public static String formatDurationFromDoubleSeconds(double totalSeconds) {
        long seconds = Math.round(totalSeconds);
        long days = seconds / 86400L;
        long hours = seconds % 86400L / 3600L;
        long minutes = seconds % 3600L / 60L;
        long remainingSeconds = seconds % 60L;
        StringBuilder formattedTime = new StringBuilder();
        if (days > 0L) {
            formattedTime.append(days).append("d ");
        }

        if (hours > 0L) {
            formattedTime.append(hours).append("h ");
        }

        if (minutes > 0L) {
            formattedTime.append(minutes).append("m ");
        }

        if (remainingSeconds > 0L || formattedTime.length() == 0) {
            formattedTime.append(remainingSeconds).append("s");
        }

        return formattedTime.toString().trim();
    }
}
