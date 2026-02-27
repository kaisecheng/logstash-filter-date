/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.logstash.filters.parser;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DecimalStyle;
import java.time.temporal.ChronoField;

/**
 * Parses ISO8601-ish date strings with nanosecond precision.
 * Supports lenient ISO8601 like Joda CasualISO8601Parser with dot and comma as decimal separators.
 * Handles timestamps with or without timezone offsets, and with or without fractional seconds (0–9 digits).
 */
public class CasualISO8601Parser implements TimestampParser {
  private static final DateTimeFormatter BASE_DOT;
  private static final DateTimeFormatter BASE_COMMA;

  static {
    BASE_DOT = new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(DateTimeFormatter.ISO_LOCAL_DATE)
        .optionalStart().appendLiteral('T').append(DateTimeFormatter.ISO_LOCAL_TIME).optionalEnd()
        // Allow space as a separator to support Joda CasualISO8601Parser's "yyyy-MM-dd HH:mm:ss"
        .optionalStart().appendLiteral(' ').append(DateTimeFormatter.ISO_LOCAL_TIME).optionalEnd()
        .optionalStart().appendZoneOrOffsetId().optionalEnd()
        .optionalStart().appendOffset("+HHmmss", "Z").optionalEnd()
        .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
        .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
        .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
        .parseDefaulting(ChronoField.NANO_OF_SECOND, 0)
        .toFormatter()
        .withZone(ZoneId.systemDefault());

    BASE_COMMA = BASE_DOT.withDecimalStyle(DecimalStyle.of(java.util.Locale.GERMAN));
  }

  private final DateTimeFormatter[] parsers;

  public CasualISO8601Parser(String timezone) {
    if (timezone == null) {
      parsers = new DateTimeFormatter[]{BASE_DOT, BASE_COMMA};
    } else {
      ZoneId zone = ZoneId.of(timezone);
      parsers = new DateTimeFormatter[]{BASE_DOT.withZone(zone), BASE_COMMA.withZone(zone)};
    }
  }

  @Override
  public Instant parse(String value) {
    RuntimeException lastException = null;
    for (DateTimeFormatter formatter : parsers) {
      try {
        return formatter.parse(value, Instant::from);
      } catch (RuntimeException e) {
        lastException = e;
      }
    }
    throw lastException;
  }

  @Override
  public Instant parse(Long value) {
    throw new IllegalArgumentException("Expected a string value, but got a long (" + value + "). Cannot parse date.");
  }

  @Override
  public Instant parse(Double value) {
    throw new IllegalArgumentException("Expected a string value, but got a double (" + value + "). Cannot parse date.");
  }

  @Override
  public Instant parse(BigDecimal value) {
    throw new IllegalArgumentException("Expected a string value, but got a bigdecimal (" + value + "). Cannot parse date.");
  }

  @Override
  public Instant parseWithTimeZone(String value, String timezone) {
    ZoneId zone = ZoneId.of(timezone);
    RuntimeException lastException = null;
    for (DateTimeFormatter formatter : parsers) {
      try {
        // withZone provides a fallback for zone-less inputs; explicit offsets in
        // the string always win over withZone().
        return formatter.withZone(zone).parse(value, Instant::from);
      } catch (RuntimeException e) {
        lastException = e;
      }
    }
    throw lastException;
  }
}
