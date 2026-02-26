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

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.After;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Locale;

import static org.junit.Assert.assertEquals;

public class JodaParserTest {

  private static Instant instant(String iso8601) {
    return ISODateTimeFormat.dateTimeParser().withZoneUTC().parseDateTime(iso8601).toInstant();
  }

  @After
  public void resetClock() {
    JodaParser.setDefaultClock(JodaParser.wallClock);
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyShouldFail() {
    new TimestampParserFactory().makeParser("", "en", "UTC");
  }

  @Test
  public void onePattern() {
    JodaParser parser = new JodaParser("YYYY", null, null);
    Instant instant = parser.parse("2016");
    assertEquals(2016, instant.toDateTime().getYear());
  }

  // Year guessing: December event arriving in January → previous year

  @Test
  public void yearGuessingDecemberInJanuary() {
    JodaParser.setDefaultClock(() -> new DateTime(2014, 1, 1, 0, 30, 50, DateTimeZone.UTC));
    JodaParser parser = new JodaParser("MMM dd HH:mm:ss", Locale.ENGLISH, "UTC");
    Instant result = parser.parse("Dec 31 23:59:00");
    assertEquals(2013, result.toDateTime(DateTimeZone.UTC).getYear());
  }

  // Year guessing: January event arriving in December → next year

  @Test
  public void yearGuessingJanuaryInDecember() {
    JodaParser.setDefaultClock(() -> new DateTime(2013, 12, 31, 23, 59, 50, DateTimeZone.UTC));
    JodaParser parser = new JodaParser("MMM dd HH:mm:ss", Locale.ENGLISH, "UTC");
    Instant result = parser.parse("Jan 01 01:00:00");
    assertEquals(2014, result.toDateTime(DateTimeZone.UTC).getYear());
  }

  // Year guessing: normal case → current year

  @Test
  public void yearGuessingNormalCaseUsesCurrentYear() {
    JodaParser.setDefaultClock(() -> new DateTime(2016, 6, 15, 12, 0, 0, DateTimeZone.UTC));
    JodaParser parser = new JodaParser("MMM dd HH:mm:ss", Locale.ENGLISH, "UTC");
    Instant result = parser.parse("Mar 01 00:00:00");
    assertEquals(2016, result.toDateTime(DateTimeZone.UTC).getYear());
  }

  // Static timezone in constructor

  @Test
  public void constructorTimezoneApplied() {
    JodaParser parser = new JodaParser("yyyy MMM dd HH:mm:ss", Locale.ENGLISH, "America/Los_Angeles");
    // LA is UTC-8 in November (standard time)
    assertEquals(instant("2013-11-24T09:29:01.000Z"), parser.parse("2013 Nov 24 01:29:01"));
  }

  @Test
  public void constructorTimezoneDSTApplied() {
    JodaParser parser = new JodaParser("yyyy MMM dd HH:mm:ss", Locale.ENGLISH, "America/Los_Angeles");
    // LA is UTC-7 in June (daylight saving time)
    assertEquals(instant("2013-06-24T08:29:01.000Z"), parser.parse("2013 Jun 24 01:29:01"));
  }

  // parseWithTimeZone overrides constructor timezone

  @Test
  public void parseWithTimeZoneOverridesCtorTimezone() {
    JodaParser parser = new JodaParser("yyyy MMM dd HH:mm:ss", Locale.ENGLISH, "UTC");
    assertEquals(instant("2013-11-24T09:29:01.000Z"),
        parser.parseWithTimeZone("2013 Nov 24 01:29:01", "America/Los_Angeles"));
  }

  // DST edge cases in CET (spring-forward: 01:59:59 → 03:00:00)

  @Test
  public void dstBeforeGapSucceeds() {
    JodaParser.setDefaultClock(() -> new DateTime(2016, 3, 29, 23, 59, 50, DateTimeZone.UTC));
    JodaParser parser = new JodaParser("MMM dd HH:mm:ss", Locale.ENGLISH, "CET");
    assertEquals(instant("2016-03-27T00:59:59.000Z"), parser.parse("Mar 27 01:59:59"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void dstGapTimeThrows() {
    JodaParser.setDefaultClock(() -> new DateTime(2016, 3, 29, 23, 59, 50, DateTimeZone.UTC));
    JodaParser parser = new JodaParser("MMM dd HH:mm:ss", Locale.ENGLISH, "CET");
    parser.parse("Mar 27 02:00:01");
  }

  @Test
  public void dstAfterGapSucceeds() {
    JodaParser.setDefaultClock(() -> new DateTime(2016, 3, 29, 23, 59, 50, DateTimeZone.UTC));
    JodaParser parser = new JodaParser("MMM dd HH:mm:ss", Locale.ENGLISH, "CET");
    assertEquals(instant("2016-03-27T01:00:01.000Z"), parser.parse("Mar 27 03:00:01"));
  }

  // Locale: parse month names in locale-specific language

  @Test
  public void parsesWithFrenchLocale() {
    JodaParser parser = new JodaParser("dd MMMM yyyy", new Locale("fr"), "UTC");
    assertEquals(instant("1789-07-14T00:00:00.000Z"), parser.parse("14 juillet 1789"));
  }

  // Rejection of non-string inputs

  @Test(expected = IllegalArgumentException.class)
  public void rejectsLong() {
    new JodaParser("yyyy", Locale.ENGLISH, "UTC").parse(1478207457L);
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsDouble() {
    new JodaParser("yyyy", Locale.ENGLISH, "UTC").parse(1478207457.456D);
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsBigDecimal() {
    new JodaParser("yyyy", Locale.ENGLISH, "UTC").parse(new BigDecimal("1478207457.456"));
  }
}
