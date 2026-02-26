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

import org.joda.time.Instant;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

public class TAI64NParserTest {

  private static Instant instant(String iso8601) {
    return ISODateTimeFormat.dateTimeParser().withZoneUTC().parseDateTime(iso8601).toInstant();
  }

  @Test
  public void parsesStringWithoutAtPrefix() {
    assertEquals(instant("2012-12-22T01:00:46.767Z"),
        new TAI64NParser().parse("4000000050d506482dbdf024"));
  }

  @Test
  public void parsesStringWithAtPrefix() {
    assertEquals(instant("2012-12-22T01:00:46.767Z"),
        new TAI64NParser().parse("@4000000050d506482dbdf024"));
  }

  @Test
  public void nanosecondComponentTruncatedToMilliseconds() {
    // 0x2dbdf024 = 767422500 ns; integer division by 1_000_000 = 767 ms
    Instant result = new TAI64NParser().parse("4000000050d506482dbdf024");
    assertEquals(767, result.toDateTime().getMillisOfSecond());
  }

  @Test
  public void parseWithTimeZoneIgnoresTimezone() {
    Instant withoutTz = new TAI64NParser().parse("4000000050d506482dbdf024");
    Instant withTz = new TAI64NParser().parseWithTimeZone("4000000050d506482dbdf024", "America/Los_Angeles");
    assertEquals(withoutTz, withTz);
  }

  // Rejection of non-string inputs

  @Test(expected = IllegalArgumentException.class)
  public void rejectsLong() {
    new TAI64NParser().parse(1356134446L);
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsDouble() {
    new TAI64NParser().parse(1356134446.767D);
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsBigDecimal() {
    new TAI64NParser().parse(new BigDecimal("1356134446.767"));
  }
}
