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
import java.util.Locale;

/**
 * Parses date strings using java.time format patterns directly.
 *
 * <p>Fractional-second letters ({@code S}) follow java.time rules: each {@code S}
 * represents exactly one decimal digit, so {@code SSS} expects exactly 3 digits.
 */
public class JavaTimeParser implements TimestampParser {
  private final DateTimeFormatter formatter;

  public JavaTimeParser(String pattern, Locale locale, String timezone) {
    ZoneId zone = (timezone != null) ? ZoneId.of(timezone) : ZoneId.systemDefault();
    this.formatter = DateTimeFormatter.ofPattern(pattern, locale != null ? locale : Locale.getDefault())
        // withZone() is a fallback for zone-less inputs
        // eg. withZone(Europe/Paris), input: "2013-11-24 01:29:01", output: "2013-11-24 00:29:01" (UTC)
        .withZone(zone);
  }

  @Override
  public Instant parse(String value) {
    return formatter.parse(value, Instant::from);
  }

  @Override
  public Instant parseWithTimeZone(String value, String timezone) {
    return formatter.withZone(ZoneId.of(timezone)).parse(value, Instant::from);
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
}
