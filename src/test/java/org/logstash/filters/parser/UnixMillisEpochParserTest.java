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
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

public class UnixMillisEpochParserTest {

  private static final long MAX_EPOCH_MILLISECONDS = (long) Integer.MAX_VALUE * 1000;

  // parse(String)

  @Test
  public void parsesStringZero() {
    assertEquals(new Instant(0), new UnixMillisEpochParser().parse("0"));
  }

  @Test
  public void parsesStringMillis() {
    assertEquals(new Instant(456), new UnixMillisEpochParser().parse("456"));
  }

  @Test
  public void parsesStringLargeMillis() {
    assertEquals(new Instant(1000000000123L), new UnixMillisEpochParser().parse("1000000000123"));
  }

  // parse(Long)

  @Test
  public void parsesLongZero() {
    assertEquals(new Instant(0), new UnixMillisEpochParser().parse(0L));
  }

  @Test
  public void parsesLongMillis() {
    assertEquals(new Instant(456), new UnixMillisEpochParser().parse(456L));
  }

  @Test
  public void parsesLongLargeMillis() {
    assertEquals(new Instant(1000000000123L), new UnixMillisEpochParser().parse(1000000000123L));
  }

  // parse(Double) — truncates to Long via Double.longValue()

  @Test
  public void parsesDoubleTruncatesToLong() {
    // 456.789D.longValue() == 456
    assertEquals(new Instant(456), new UnixMillisEpochParser().parse(456.789D));
  }

  // parse(BigDecimal)

  @Test
  public void parsesBigDecimalMillis() {
    assertEquals(new Instant(456), new UnixMillisEpochParser().parse(new BigDecimal("456")));
  }

  @Test
  public void parsesBigDecimalAtMaxBoundary() {
    assertEquals(new Instant(MAX_EPOCH_MILLISECONDS),
        new UnixMillisEpochParser().parse(new BigDecimal(MAX_EPOCH_MILLISECONDS)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void rejectsBigDecimalAboveMax() {
    new UnixMillisEpochParser().parse(new BigDecimal(MAX_EPOCH_MILLISECONDS + 1));
  }

  // parseWithTimeZone ignores timezone

  @Test
  public void parseWithTimeZoneIgnoresTimezone() {
    assertEquals(new UnixMillisEpochParser().parse("1000000000123"),
        new UnixMillisEpochParser().parseWithTimeZone("1000000000123", "America/Los_Angeles"));
  }
}
