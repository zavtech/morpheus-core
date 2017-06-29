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
package com.zavtech.morpheus.util.text;

import java.text.FieldPosition;
import java.text.ParsePosition;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.text.DecimalFormat;
import java.text.DateFormat;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Date;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * A Format that tries to be smart about parsing and formatting various data types.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class SmartFormat extends java.text.Format {

    private static final String INTEGER_REGEX = "[-+]?[0-9]+";
    private static final String DOUBLE_REGEX = "[-+]?[0-9]*\\.?[0-9]+";
    private static final String PERCENT_REGEX = "([-+]?[0-9]*\\.?[0-9]+)%";
    private static final String SCIENTIFIC_REGEX = "[-+]?[0-9]*\\.?[0-9]*([Ee][+-]?[0-9]+)?";
    private static final String DATE1_REGEX = "(\\d{4})-(\\d{2})-(\\d{2})";
    private static final String DATE2_REGEX = "(\\d{2})-(\\d{2})-(\\d{4})";
    private static final String DATE3_REGEX = "(\\d{1,2})/(\\d{1,2})/(\\d{4})";
    private static final String DATE4_REGEX = "(\\d{2})-([A-Za-z]{3})-(\\d{4})";
    private static final String TIME_REGEX_1 = "(\\d{1,2}):(\\d{1,2})";
    private static final String TIME_REGEX_2 = "(\\d{1,2}):(\\d{1,2})(am|pm)";

    private Matcher integerMatcher = Pattern.compile(INTEGER_REGEX).matcher("");
    private Matcher doubleMatcher = Pattern.compile(DOUBLE_REGEX).matcher("");
    private Matcher percentMatcher = Pattern.compile(PERCENT_REGEX).matcher("");
    private Matcher scientificMatcher = Pattern.compile(SCIENTIFIC_REGEX).matcher("");

    private Matcher date1Matcher = Pattern.compile(DATE1_REGEX).matcher("");
    private Matcher date2Matcher = Pattern.compile(DATE2_REGEX).matcher("");
    private Matcher date3Matcher = Pattern.compile(DATE3_REGEX).matcher("");
    private Matcher date4Matcher = Pattern.compile(DATE4_REGEX).matcher("");

    private Matcher timeMatcher1 = Pattern.compile(TIME_REGEX_1).matcher("");
    private Matcher timeMatcher2 = Pattern.compile(TIME_REGEX_2).matcher("");

    private Format decimalFormat1 = new DecimalFormat("0.0000####;-0.0000####");
    private Format decimalFormat2 = new DecimalFormat("0.0000####;-0.0000####");
    private Format decimalFormat6 = new DecimalFormat("0.0000####;-0.0000####");
    private DateFormat dateFormat1 = new SimpleDateFormat("yyyy-MM-dd");
    private DateFormat dateFormat2 = new SimpleDateFormat("dd-MM-yyyy");
    private DateFormat dateFormat3 = new SimpleDateFormat("dd/MM/yyyy");
    private DateFormat dateFormat4 = new SimpleDateFormat("dd-MMM-yyyy");

    /**
     * Constructor
     */
    public SmartFormat() {
        this(TimeZone.getDefault());
    }

    /**
     * Constructor
     * @param timeZone  the time zone
     */
    public SmartFormat(TimeZone timeZone) {
        this.setTimeZone(timeZone);
    }

    /**
     * Sets the time zone for all date formats
     * @param timeZone      the time zone for date formats
     */
    public void setTimeZone(TimeZone timeZone) {
        this.dateFormat1.setTimeZone(timeZone);
        this.dateFormat2.setTimeZone(timeZone);
        this.dateFormat3.setTimeZone(timeZone);
        this.dateFormat4.setTimeZone(timeZone);
    }

    @Override()
    public StringBuffer format(Object value, StringBuffer buffer, FieldPosition position) {
        if (value == null) {
            buffer.append("null");
            return buffer;
        } else if (value instanceof String) {
            buffer.append(value.toString());
            return buffer;
        } else if (value instanceof Double) {
            final Double doubleValue = (Double)value;
            if (Double.isNaN(doubleValue)) buffer.append("NaN");
            else if (doubleValue < 0.001d) buffer.append(decimalFormat1.format(value));
            else if (doubleValue < 0d) buffer.append(decimalFormat6.format(value));
            else if (doubleValue > 1000000d) buffer.append(decimalFormat1.format(value));
            else buffer.append(decimalFormat2.format(value));
            return buffer;
        } else if (value instanceof Boolean) {
            buffer.append(value.toString());
            return buffer;
        } else if (value instanceof Integer) {
            buffer.append(value.toString());
            return buffer;
        } else if (value instanceof Long) {
            buffer.append(value.toString());
            return buffer;
        } else if (value instanceof Date) {
            buffer.append(dateFormat1.format(value));
            return buffer;
        } else if (value instanceof Calendar) {
            final Calendar calendar = (Calendar)value;
            buffer.append(dateFormat1.format(calendar.getTime()));
            return buffer;
        } else if (value instanceof Float) {
            buffer.append(value.toString());
            return buffer;
        } else if (value instanceof Short) {
            buffer.append(value.toString());
            return buffer;
        } else {
            buffer.append(value.toString());
            return buffer;
        }
    }

    @Override()
    public Object parseObject(String value, ParsePosition position) {
        try {
            if (value == null || value.length() == 0 || value.equalsIgnoreCase("N/A") || value.equalsIgnoreCase("null")) {
                position.setIndex(value != null && value.trim().length() > 0 ? value.length() : 1);
                return null;
            } else if (value.equalsIgnoreCase("NaN")) {
                position.setIndex(value.length());
                return Double.NaN;
            } else if (doubleMatcher.reset(value).matches()) {
                position.setIndex(value.length());
                return Double.parseDouble(value);
            } else if (percentMatcher.reset(value).matches()) {
                position.setIndex(value.length());
                return Double.parseDouble(percentMatcher.group(1)) / 100d;
            } else if (scientificMatcher.reset(value).matches()) {
                position.setIndex(value.length());
                return Double.parseDouble(value);
            } else if (integerMatcher.reset(value).matches()) {
                position.setIndex(value.length());
                return value.length() > 8 ? Long.parseLong(value) : Integer.parseInt(value);
            } else if (date1Matcher.reset(value).matches()) {
                return dateFormat1.parseObject(value, position);
            } else if (date2Matcher.reset(value).matches()) {
                return dateFormat2.parseObject(value, position);
            } else if (date3Matcher.reset(value).matches()) {
                return dateFormat3.parseObject(value, position);
            } else if (date4Matcher.reset(value).matches()) {
                return dateFormat4.parseObject(value, position);
            } else if (timeMatcher1.reset(value).matches()) {
                position.setIndex(value.length());
                final int hour = Integer.parseInt(timeMatcher1.group(1));
                final int min = Integer.parseInt(timeMatcher1.group(2));
                final Calendar calendar = Calendar.getInstance();
                calendar.set(Calendar.HOUR, hour);
                calendar.set(Calendar.MINUTE, min);
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.MILLISECOND, 0);
                return calendar.getTime();
            } else if (timeMatcher2.reset(value).matches()) {
                position.setIndex(value.length());
                final int hour = Integer.parseInt(timeMatcher2.group(1));
                final int min = Integer.parseInt(timeMatcher2.group(2));
                final boolean pm = timeMatcher2.group(3).equalsIgnoreCase("PM");
                final Calendar calendar = Calendar.getInstance();
                calendar.set(Calendar.AM_PM, pm ? Calendar.PM : Calendar.AM);
                calendar.set(Calendar.HOUR_OF_DAY, hour);
                calendar.set(Calendar.MINUTE, min);
                calendar.set(Calendar.SECOND, 0);
                calendar.set(Calendar.MILLISECOND, 0);
                return calendar.getTime();
            } else {
                position.setIndex(value.length());
                return value;
            }
        } catch (Throwable t) {
            throw new RuntimeException("Failed to parse value '" + value + "'", t);
        }
    }

    public static void main(String[] args) {
        final int count = 20;
        final long t1 = System.currentTimeMillis();
        for (int i=0; i<count; ++i) {
            final String uuid = UUID.randomUUID().toString();
            System.out.println(uuid);
        }
        final long t2 = System.currentTimeMillis();
        System.out.println("Created " + count + " UUIDs in " + (t2-t1) + " millis");
    }

}
