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

public class UnixEpochParserTest {

  private static Instant instant(long epochSeconds, long nanos) {
    return Instant.ofEpochSecond(epochSeconds, nanos);
  }

  // parse(String) — integer seconds

  @Test
  public void parsesIntegerStringSeconds() {
    assertEquals(instant(1478207457, 0), new UnixEpochParser().parse("1478207457"));
  }

  // parse(String) — 3 fractional digits (milliseconds)

  @Test
  public void parsesStringWithMillisecondFraction() {
    assertEquals(instant(1478207457, 123_000_000), new UnixEpochParser().parse("1478207457.123"));
  }

  // parse(String) — 6 fractional digits (microseconds)

  @Test
  public void parsesStringWithMicrosecondFraction() {
    assertEquals(instant(1478207457, 123_456_000), new UnixEpochParser().parse("1478207457.123456"));
  }

  // parse(String) — 9 fractional digits (nanoseconds)

  @Test
  public void parsesStringWithNanosecondFraction() {
    assertEquals(instant(1478207457, 123_456_789), new UnixEpochParser().parse("1478207457.123456789"));
  }

  // parse(String) — >9 fractional digits are silently truncated to 9

  @Test
  public void parsesStringTruncatesDigitsBeyondNine() {
    assertEquals(instant(1478207457, 123_456_789), new UnixEpochParser().parse("1478207457.1234567890"));
  }

  // parse(Long)

  @Test
  public void parsesLongZero() {
    assertEquals(instant(0, 0), new UnixEpochParser().parse(0L));
  }

  @Test
  public void parsesLongKnownValue() {
    assertEquals(instant(1478207457, 0), new UnixEpochParser().parse(1478207457L));
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsLongAboveMaxEpochSeconds() {
    new UnixEpochParser().parse((long) Integer.MAX_VALUE + 1L);
  }

  // parse(Double) — limited to approximately microsecond precision

  @Test
  public void parsesDoubleWithFractionalSeconds() {
    assertEquals(instant(1478207457, 456_000_000), new UnixEpochParser().parse(1478207457.456D));
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsDoubleAboveMaxEpochSeconds() {
    new UnixEpochParser().parse((double) Integer.MAX_VALUE + 1.0);
  }

  // parse(BigDecimal) — full nanosecond precision

  @Test
  public void parsesBigDecimalWithFullNanosecondPrecision() {
    assertEquals(instant(1478207457, 123_456_789),
        new UnixEpochParser().parse(new BigDecimal("1478207457.123456789")));
  }

  // parseWithTimeZone is a no-op (epoch is always UTC)

  @Test
  public void parseWithTimeZoneIgnoresTimezone() {
    assertEquals(instant(1478207457, 123_456_789),
        new UnixEpochParser().parseWithTimeZone("1478207457.123456789", "America/Los_Angeles"));
  }
}
