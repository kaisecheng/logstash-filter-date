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

import static org.junit.Assert.assertEquals;

public class CasualISO8601ParserTest {

  private static Instant instant(String iso8601) {
    return Instant.parse(iso8601);
  }

  // T-separator with zone offset

  @Test
  public void parsesISO8601WithNegativeOffset() {
    assertEquals(instant("2001-01-01T08:00:00Z"),
        new CasualISO8601Parser(null).parse("2001-01-01T00:00:00-0800"));
  }

  @Test
  public void parsesISO8601WithColonOffset() {
    assertEquals(instant("2010-05-03T08:18:18Z"),
        new CasualISO8601Parser(null).parse("2010-05-03T08:18:18+00:00"));
  }

  @Test
  public void parsesISO8601WithMillisAndPositiveOffset() {
    assertEquals(instant("2001-09-05T09:36:36.123Z"),
        new CasualISO8601Parser(null).parse("2001-09-05T16:36:36.123+0700"));
  }

  @Test
  public void parsesISO8601WithNanoseconds() {
    assertEquals(instant("2001-09-05T09:36:36.123456789Z"),
        new CasualISO8601Parser(null).parse("2001-09-05T16:36:36.123456789+0700"));
  }

  @Test
  public void parsesISO8601WithCommaDecimalAndZSuffix() {
    assertEquals(instant("2001-12-07T23:54:54.123Z"),
        new CasualISO8601Parser(null).parse("2001-12-07T23:54:54,123Z"));
  }

  // Space-separator with zone

  @Test
  public void parsesSpaceSeparatedWithDotAndZSuffix() {
    assertEquals(instant("2001-12-07T23:54:54.123Z"),
        new CasualISO8601Parser(null).parse("2001-12-07 23:54:54.123Z"));
  }

  @Test
  public void parsesSpaceSeparatedWithCommaAndZSuffix() {
    assertEquals(instant("2001-12-07T23:54:54.123Z"),
        new CasualISO8601Parser(null).parse("2001-12-07 23:54:54,123Z"));
  }

  // Space-separator with no zone — constructor timezone applied

  @Test
  public void parsesSpaceSeparatedWithDotNoZone() {
    assertEquals(instant("2001-11-06T20:45:45.123Z"),
        new CasualISO8601Parser("UTC").parse("2001-11-06 20:45:45.123"));
  }

  @Test
  public void parsesSpaceSeparatedWithCommaNoZone() {
    assertEquals(instant("2001-11-06T20:45:45.123Z"),
        new CasualISO8601Parser("UTC").parse("2001-11-06 20:45:45,123"));
  }

  // No-zone variants apply constructor timezone

  @Test
  public void constructorTimezoneAppliedToNoZoneString() {
    // America/Caracas was UTC-4:00 in 2001
    assertEquals(instant("2001-01-01T04:00:00Z"),
        new CasualISO8601Parser("America/Caracas").parse("2001-01-01T00:00:00"));
  }

  @Test
  public void constructorTimezoneReflectsHistoricalOffset() {
    // Venezuela changed to UTC-4:30 in late 2007; 2008 uses the new offset
    assertEquals(instant("2008-01-01T04:30:00Z"),
        new CasualISO8601Parser("America/Caracas").parse("2008-01-01T00:00:00"));
  }

  // parseWithTimeZone

  @Test
  public void parseWithTimeZoneAppliesDynamicTimezone() {
    assertEquals(instant("2001-01-01T04:00:00Z"),
        new CasualISO8601Parser(null).parseWithTimeZone("2001-01-01T00:00:00", "America/Caracas"));
  }

  @Test
  public void parseWithTimeZoneHandlesHistoricalOffset() {
    // Venezuela changed from -4:30 to -4:00 on 1 May 2016
    assertEquals(instant("2016-05-01T12:18:18.123Z"),
        new CasualISO8601Parser(null).parseWithTimeZone("2016-05-01T08:18:18.123", "America/Caracas"));
  }

  // Rejection of non-string inputs

  @Test(expected = IllegalArgumentException.class)
  public void rejectsLong() {
    new CasualISO8601Parser(null).parse(1478207457L);
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsDouble() {
    new CasualISO8601Parser(null).parse(1478207457.456D);
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsBigDecimal() {
    new CasualISO8601Parser(null).parse(new BigDecimal("1478207457.456"));
  }
}
