package id.labs247.medan.newsfetcher.utils;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;

public class DateUtils {

    private static final List<DateTimeFormatter> formatters = Arrays.asList(
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX"),     
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),  
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ"),        
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),        
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm"),            
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),           
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss'Z'"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mmZ")    
    );

    public static String dateFormatter(LocalDateTime localDateTime, String format) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
        return localDateTime.format(formatter);
    }

    public static String dateFormatter(String date, String currentFormat, String targetFormat) {
        DateTimeFormatter currentFormatter = DateTimeFormatter.ofPattern(currentFormat);
        java.time.LocalDate localDate = java.time.LocalDate.parse(date, currentFormatter);
        DateTimeFormatter targetFormatter = DateTimeFormatter.ofPattern(targetFormat);
        return localDate.format(targetFormatter);
    }

    public static String parseDatetimeToStandardFormat(String dateString, String timezone) {
        for (DateTimeFormatter formatter : formatters) {
            try {
                OffsetDateTime odt = OffsetDateTime.parse(dateString, formatter);
                return odt.atZoneSameInstant(ZoneId.of(timezone)).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            } catch (DateTimeParseException e) {
                try {
                    LocalDateTime ldt = LocalDateTime.parse(dateString, formatter);
                    return ldt.atZone(ZoneId.of(timezone)).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                } catch (DateTimeParseException ignored) {
                }
            }
        }
        return dateString;
    }

    public static String parseDatetimeToDateOnly(String dateString) {
        for (DateTimeFormatter formatter : formatters) {
            try {
                OffsetDateTime odt = OffsetDateTime.parse(dateString, formatter);
                return odt.toLocalDate().toString();
            } catch (DateTimeParseException e) {
            }
            try {
                LocalDateTime ldt = LocalDateTime.parse(dateString, formatter);
                return ldt.toLocalDate().toString();
            } catch (DateTimeParseException e) {
            }
        }
        return dateString;
    }

    public static String cleanAndParseDatetime(String dateString) {
        if(dateString.contains("WIB")) {
            dateString = dateString.replace("WIB", "T");
            return dateString;
        } else {
            return dateString;
        }
    }

    public static String standarizeDatetime(String dateString, String timezone) {
        dateString = cleanAndParseDatetime(dateString);
        dateString = parseDatetimeToStandardFormat(dateString, timezone);
        return dateString;
    }

    public static ZonedDateTime convertDatetimeToUTC(String date) {
        OffsetDateTime odt = OffsetDateTime.parse(date, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        return odt.toInstant().atZone(ZoneOffset.UTC);

    }

    public static Long parseTimestamp(String timestampString) {
        for (DateTimeFormatter formatter : formatters) {
            try {
                OffsetDateTime dateTime = OffsetDateTime.parse(timestampString, formatter);
                return dateTime.toInstant().toEpochMilli(); // Convert to milliseconds since epoch
            } catch (Exception e) {
                continue;
            }
        }
        return null; // Return null if no formatter works
    }

    public static boolean matchFormatter(String dateString) {
        for (DateTimeFormatter formatter : formatters) {
            try {
                OffsetDateTime.parse(dateString, formatter);
                return true;
            } catch (DateTimeParseException e) {
                try {
                    LocalDateTime.parse(dateString, formatter);
                    return true;
                } catch (DateTimeParseException ignored) {
                }
            }
        }
        return false; // Return false if no formatter matched
    }
}
