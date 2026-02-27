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

import org.junit.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Locale;

import static org.junit.Assert.assertEquals;

public class JavaTimeParserTest {

  private static Instant instant(String iso8601) {
    return Instant.parse(iso8601);
  }

  // Basic pattern with explicit timezone in constructor

  @Test
  public void parsesPatternWithConstructorTimezone() {
    // UTC-8 in November (standard time) for America/Los_Angeles
    JavaTimeParser parser = new JavaTimeParser("yyyy MMM dd HH:mm:ss", Locale.ENGLISH, "America/Los_Angeles");
    assertEquals(instant("2013-11-24T09:29:01Z"), parser.parse("2013 Nov 24 01:29:01"));
  }

  @Test
  public void parsesPatternWithDSTTimezone() {
    // UTC-7 in June (daylight saving time) for America/Los_Angeles
    JavaTimeParser parser = new JavaTimeParser("yyyy MMM dd HH:mm:ss", Locale.ENGLISH, "America/Los_Angeles");
    assertEquals(instant("2013-06-24T08:29:01Z"), parser.parse("2013 Jun 24 01:29:01"));
  }

  // Nanosecond precision with 9S

  @Test
  public void parsesNanosecondPattern() {
    JavaTimeParser parser = new JavaTimeParser("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'", Locale.ENGLISH, "UTC");
    assertEquals(instant("2016-11-03T21:10:57.123456789Z"), parser.parse("2016-11-03T21:10:57.123456789Z"));
  }

  // VV (zone ID) embedded in the string overrides the constructor timezone

  @Test
  public void parsesPatternWithEmbeddedZoneId() {
    JavaTimeParser parser = new JavaTimeParser("yyyy-MM-dd HH:mm:ss VV", Locale.ENGLISH, null);
    // LA is UTC-8 in November (standard time)
    assertEquals(instant("2013-11-24T09:29:01Z"), parser.parse("2013-11-24 01:29:01 America/Los_Angeles"));
  }

  // parseWithTimeZone overrides constructor timezone

  @Test
  public void parseWithTimeZoneOverridesConstructorTimezone() {
    JavaTimeParser parser = new JavaTimeParser("yyyy MMM dd HH:mm:ss", Locale.ENGLISH, "UTC");
    assertEquals(instant("2013-11-24T09:29:01Z"),
        parser.parseWithTimeZone("2013 Nov 24 01:29:01", "America/Los_Angeles"));
  }

  // Locale: parse month names in a specific language (pattern must include time for Instant resolution)

  @Test
  public void parsesWithFrenchLocale() {
    JavaTimeParser parser = new JavaTimeParser("dd MMMM yyyy HH:mm:ss", new Locale("fr"), "UTC");
    assertEquals(instant("1789-07-14T09:00:00Z"), parser.parse("14 juillet 1789 09:00:00"));
  }

  // Rejection of non-string inputs

  @Test(expected = IllegalArgumentException.class)
  public void rejectsLong() {
    new JavaTimeParser("yyyy", Locale.ENGLISH, "UTC").parse(1478207457L);
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsDouble() {
    new JavaTimeParser("yyyy", Locale.ENGLISH, "UTC").parse(1478207457.456D);
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsBigDecimal() {
    new JavaTimeParser("yyyy", Locale.ENGLISH, "UTC").parse(new BigDecimal("1478207457.456"));
  }
}
