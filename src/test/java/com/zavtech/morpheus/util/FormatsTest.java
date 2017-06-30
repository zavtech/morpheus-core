/**
 * Copyright (C) 2014-2017 Xavier Witdouck
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.zavtech.morpheus.util;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.TimeZone;

import static org.testng.Assert.*;
import org.testng.annotations.Test;

import com.zavtech.morpheus.util.text.Formats;

/**
 * Unit tests for the Formats class
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class FormatsTest {

    @Test()
    public void testDefaultParsers() {
        final Formats formats = new Formats();
        assertEquals(formats.getParserOrFail(boolean.class).apply("true"), Boolean.TRUE);
        assertEquals(formats.getParserOrFail(boolean.class).apply("false"), Boolean.FALSE);
        assertEquals(formats.getParserOrFail(Boolean.class).apply("true"), Boolean.TRUE);
        assertEquals(formats.getParserOrFail(Boolean.class).apply("false"), Boolean.FALSE);
        assertEquals(formats.getParserOrFail(Integer.class).apply("15"), 15);
        assertEquals(formats.getParserOrFail(int.class).apply("15"), 15);
        assertEquals(formats.getParserOrFail(Long.class).apply("2343"), 2343L);
        assertEquals(formats.getParserOrFail(long.class).apply("2343"), 2343L);
        assertEquals(formats.getParserOrFail(Double.class).apply("34.233435"), 34.233435d);
        assertEquals(formats.getParserOrFail(double.class).apply("34.233435"), 34.233435d);
        assertEquals(formats.getParserOrFail(String.class).apply("Hello there!"), "Hello there!");
        assertEquals(formats.getParserOrFail(ZoneId.class).apply("Europe/London"), ZoneId.of("Europe/London"));
        assertEquals(formats.getParserOrFail(TimeZone.class).apply("Europe/London"), TimeZone.getTimeZone("Europe/London"));
        assertEquals(formats.getParserOrFail(LocalDate.class).apply("2014-05-22"), LocalDate.of(2014, 5, 22));
        assertEquals(formats.getParserOrFail(LocalDateTime.class).apply("2014-05-22T22:34:00"), LocalDateTime.of(2014, 5, 22, 22, 34, 0));
        assertEquals(formats.getParserOrFail(ZonedDateTime.class).apply("2014-05-22T22:34:00-04:00[America/New_York]"), ZonedDateTime.of(2014, 5, 22, 22, 34, 0, 0, ZoneId.of("America/New_York")));
    }

    @Test()
    public void testDefaultPrinters() {
        final Formats formats = new Formats();
        assertEquals(formats.getPrinterOrFail(boolean.class).apply(true), "true");
        assertEquals(formats.getPrinterOrFail(boolean.class).apply(false), "false");
        assertEquals(formats.getPrinterOrFail(Boolean.class).apply(true), "true");
        assertEquals(formats.getPrinterOrFail(Boolean.class).apply(false), "false");
        assertEquals(formats.getPrinterOrFail(Integer.class).apply(15), "15");
        assertEquals(formats.getPrinterOrFail(int.class).apply(15), "15");
        assertEquals(formats.getPrinterOrFail(Long.class).apply(2343L), "2343");
        assertEquals(formats.getPrinterOrFail(long.class).apply(2343L), "2343");
        assertEquals(formats.getPrinterOrFail(Double.class).apply(34.233435d), "34.233435");
        assertEquals(formats.getPrinterOrFail(double.class).apply(34.233435d), "34.233435");
        assertEquals(formats.getPrinterOrFail(String.class).apply("Hello there!"), "Hello there!");
        assertEquals(formats.getPrinterOrFail(ZoneId.class).apply(ZoneId.of("Europe/London")), "Europe/London");
        assertEquals(formats.getPrinterOrFail(TimeZone.class).apply(TimeZone.getTimeZone("Europe/London")), "Europe/London");
        assertEquals(formats.getPrinterOrFail(LocalDate.class).apply(LocalDate.of(2014, 5, 22)), "2014-05-22");
        assertEquals(formats.getPrinterOrFail(LocalDateTime.class).apply(LocalDateTime.of(2014, 5, 22, 22, 34, 0)), "2014-05-22T22:34:00");
        assertEquals(formats.getPrinterOrFail(ZonedDateTime.class).apply(ZonedDateTime.of(2014, 5, 22, 22, 34, 0, 0, ZoneId.of("America/New_York"))), "2014-05-22T22:34:00-04:00[America/New_York]");
    }

    @Test
    public void testObjectParser() {
        final Formats formats = new Formats();
        assertEquals(formats.getParserOrFail(Object.class).apply("true"), true);
        assertEquals(formats.getParserOrFail(Object.class).apply("123"), 123);
        assertEquals(formats.getParserOrFail(Object.class).apply("1234567"), 1234567);
        assertEquals(formats.getParserOrFail(Object.class).apply("34.54546"), 34.54546d);
        assertEquals(formats.getParserOrFail(Object.class).apply("34.54546"), 34.54546d);
        assertEquals(formats.getParserOrFail(Object.class).apply("Europe/London"), ZoneId.of("Europe/London"));
        assertEquals(formats.getParserOrFail(Object.class).apply("2014-05-22"), LocalDate.of(2014, 5, 22));
        assertEquals(formats.getParserOrFail(Object.class).apply("18:35"), LocalTime.of(18, 35));
        assertEquals(formats.getParserOrFail(Object.class).apply("2014-05-22T05:00:00"), LocalDateTime.of(2014, 5, 22, 5, 0, 0));
    }

    @Test
    public void testParseNulls() {
        final Formats formats = new Formats();
        assertEquals(formats.getParserOrFail(boolean.class).apply(null), Boolean.FALSE);
        assertEquals(formats.getParserOrFail(boolean.class).apply(null), Boolean.FALSE);
        assertEquals(formats.getParserOrFail(Boolean.class).apply(null), Boolean.FALSE);
        assertEquals(formats.getParserOrFail(Boolean.class).apply(null), Boolean.FALSE);
        assertEquals(formats.getParserOrFail(Integer.class).apply(null), 0);
        assertEquals(formats.getParserOrFail(int.class).apply(null), 0);
        assertEquals(formats.getParserOrFail(Long.class).apply(null), 0L);
        assertEquals(formats.getParserOrFail(long.class).apply(null), 0L);
        assertEquals(formats.getParserOrFail(Double.class).apply(null), Double.NaN);
        assertEquals(formats.getParserOrFail(double.class).apply(null), Double.NaN);
        assertEquals(formats.getParserOrFail(String.class).apply(null), null);
        assertEquals(formats.getParserOrFail(ZoneId.class).apply(null), null);
        assertEquals(formats.getParserOrFail(TimeZone.class).apply(null), null);
        assertEquals(formats.getParserOrFail(LocalDate.class).apply(null), null);
        assertEquals(formats.getParserOrFail(LocalDateTime.class).apply(null), null);
        assertEquals(formats.getParserOrFail(ZonedDateTime.class).apply(null), null);
    }

    @Test
    public void testParseNullsWithSpecificCharacter() {
        final Formats formats = new Formats();
        formats.setNullValues("null", "-");
        assertEquals(formats.getParserOrFail(boolean.class).apply("-"), Boolean.FALSE);
        assertEquals(formats.getParserOrFail(boolean.class).apply("-"), Boolean.FALSE);
        assertEquals(formats.getParserOrFail(Boolean.class).apply("-"), Boolean.FALSE);
        assertEquals(formats.getParserOrFail(Boolean.class).apply("-"), Boolean.FALSE);
        assertEquals(formats.getParserOrFail(Integer.class).apply("-"), 0);
        assertEquals(formats.getParserOrFail(int.class).apply("-"), 0);
        assertEquals(formats.getParserOrFail(Long.class).apply("-"), 0L);
        assertEquals(formats.getParserOrFail(long.class).apply("-"), 0L);
        assertEquals(formats.getParserOrFail(Double.class).apply("-"), Double.NaN);
        assertEquals(formats.getParserOrFail(double.class).apply("-"), Double.NaN);
        assertEquals(formats.getParserOrFail(String.class).apply("-"), null);
        assertEquals(formats.getParserOrFail(ZoneId.class).apply("-"), null);
        assertEquals(formats.getParserOrFail(TimeZone.class).apply("-"), null);
        assertEquals(formats.getParserOrFail(LocalDate.class).apply("-"), null);
        assertEquals(formats.getParserOrFail(LocalDateTime.class).apply("-"), null);
        assertEquals(formats.getParserOrFail(ZonedDateTime.class).apply("-"), null);
    }


    @Test
    public void testCustomParser() {
        final Formats formats = new Formats();
        formats.copyParser(Double.class, "MyKey");
        assertEquals(formats.getParserOrFail("MyKey").apply("34"), 34d);
        assertEquals(formats.getParserOrFail("MyKey").apply(null), Double.NaN);
    }


}
