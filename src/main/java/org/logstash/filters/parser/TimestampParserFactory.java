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

import java.util.Locale;

public class TimestampParserFactory {
  public static final String PRECISION_NS = "ns";
  public static final String PRECISION_MS = "ms";
  private static final String ISO8601 = "ISO8601";
  private static final String UNIX = "UNIX";
  private static final String UNIX_MS = "UNIX_MS";
  private static final String TAI64N = "TAI64N";

  /*
   * zone is a String because it can be dynamic and come from the event while we parse it.
   */
  public static TimestampParser makeParser(String pattern, String locale, String zone, String precision) {
    return makeParser(pattern, locale == null ? Locale.getDefault() : Locale.forLanguageTag(locale), zone, precision);
  }

  private static TimestampParser makeParser(String pattern, Locale locale, String zone, String precision) {
    // If zone contains "%{", it is dynamic and will be resolved per-event via parseWithTimeZone.
    String tz = (zone == null || zone.contains("%{")) ? null : zone;

    switch (pattern) {
      case ISO8601: return new CasualISO8601Parser(tz);
      case UNIX:    return new UnixEpochParser();
      case UNIX_MS: return new UnixMillisEpochParser();
      case TAI64N:  return new TAI64NParser();
      default:
        return PRECISION_NS.equals(precision)
            ? new JavaTimeParser(pattern, locale, tz)
            : new JodaParser(pattern, locale, tz);
    }
  }
}
