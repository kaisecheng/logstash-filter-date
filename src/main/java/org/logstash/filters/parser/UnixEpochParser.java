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

/**
 * Parses Unix epoch timestamps with nanosecond precision.
 *
 * <p>String inputs support up to 9 fractional digits (nanoseconds).
 * Double inputs are limited to approximately microsecond precision due to
 * IEEE 754 representation (~15-16 significant digits; epoch-seconds use ~10).
 * Use String or BigDecimal for full nanosecond precision.
 */
public class UnixEpochParser implements TimestampParser {
  private static final long MAX_EPOCH_SECONDS = Integer.MAX_VALUE;

  @Override
  public Instant parse(String value) {
    if (value.contains(".")) {
      int dot = value.indexOf(".");
      long seconds = Long.parseLong(value.substring(0, dot));
      checkMaxSeconds(seconds);
      // Support up to 9 fractional digits, right-padding the fractional part with zeros
      // Digits beyond the 9th are silently ignored
      int subdigits = Math.min(9, value.length() - dot - 1);
      long nanos = Long.parseLong(value.substring(dot + 1, dot + 1 + subdigits));
      for (int i = subdigits; i < 9; i++) {
        nanos *= 10;
      }
      return Instant.ofEpochSecond(seconds, nanos);
    } else {
      return parse(Long.parseLong(value));
    }
  }

  @Override
  public Instant parse(Long value) {
    checkMaxSeconds(value);
    return Instant.ofEpochSecond(value);
  }

  @Override
  public Instant parse(Double value) {
    long seconds = value.longValue();
    checkMaxSeconds(seconds);
    // Doubles have ~15-16 significant digits; epoch-seconds values around 1e9 leave only ~6-7
    // digits for the fractional part, giving at best microsecond precision.
    long micros = (long) (value * 1_000_000);
    return Instant.ofEpochSecond(seconds, (micros % 1_000_000) * 1_000);
  }

  @Override
  public Instant parse(BigDecimal value) {
    long seconds = value.longValue();
    checkMaxSeconds(seconds);
    long nanos = value.subtract(BigDecimal.valueOf(seconds))
        .scaleByPowerOfTen(9)
        .longValue();
    return Instant.ofEpochSecond(seconds, nanos);
  }

  @Override
  public Instant parseWithTimeZone(String value, String timezone) {
    return parse(value);
  }

  private void checkMaxSeconds(long value) {
    if (value > MAX_EPOCH_SECONDS) {
      throw new IllegalArgumentException("Cannot parse date for value larger than UNIX epoch maximum seconds");
    }
  }
}
