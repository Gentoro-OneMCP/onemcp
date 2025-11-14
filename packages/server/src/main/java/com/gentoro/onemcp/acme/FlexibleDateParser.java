package com.gentoro.onemcp.acme;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.List;

public class FlexibleDateParser {
  private static final List<DateTimeFormatter> KNOWN_FORMATS =
      List.of(
          DateTimeFormatter.ISO_INSTANT, // e.g. 2024-05-01T00:00:00Z
          DateTimeFormatter.ISO_OFFSET_DATE_TIME, // e.g. 2024-05-01T00:00:00+02:00
          DateTimeFormatter.ISO_ZONED_DATE_TIME, // e.g. 2024-05-01T00:00:00+02:00[Europe/Paris]
          DateTimeFormatter.ISO_LOCAL_DATE_TIME, // e.g. 2024-05-01T00:00:00
          DateTimeFormatter.ISO_LOCAL_DATE // e.g. 2024-05-01
          );

  public static Instant parseFlexible(String input) {
    for (DateTimeFormatter fmt : KNOWN_FORMATS) {
      try {
        TemporalAccessor parsed = fmt.parse(input);
        // Convert to Instant if possible
        if (parsed instanceof Instant) return (Instant) parsed;
        if (parsed.query(TemporalQueries.offset()) != null)
          return OffsetDateTime.from(parsed).toInstant();
        if (parsed.query(TemporalQueries.zone()) != null)
          return ZonedDateTime.from(parsed).toInstant();
        if (parsed.isSupported(ChronoField.HOUR_OF_DAY))
          return LocalDateTime.from(parsed).toInstant(ZoneOffset.UTC);
        if (parsed.isSupported(ChronoField.DAY_OF_MONTH))
          return LocalDate.from(parsed).atStartOfDay(ZoneOffset.UTC).toInstant();
      } catch (DateTimeParseException ignored) {
        // Try the next format
      }
    }
    throw new IllegalArgumentException("Unrecognized date format: " + input);
  }

  public static void main(String[] args) {
    String[] samples = {
      "2024-05-01",
      "2024-05-01T00:00:00",
      "2024-05-01T00:00:00Z",
      "2024-05-01T00:00:00+02:00",
      "2024-05-01T00:00:00+02:00[Europe/Paris]"
    };

    for (String s : samples) {
      System.out.println(s + " → " + parseFlexible(s));
    }
  }
}
