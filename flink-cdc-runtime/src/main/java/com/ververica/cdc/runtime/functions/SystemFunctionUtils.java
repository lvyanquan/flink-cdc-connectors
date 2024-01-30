/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.runtime.functions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.WeekFields;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** System function utils to support the call of flink cdc pipeline transform. */
public class SystemFunctionUtils {
    private static final Logger LOG = LoggerFactory.getLogger(SystemFunctionUtils.class);

    public static long localtime() {
        return LocalTime.now().toSecondOfDay();
    }

    public static long localtimestamp() {
        return Instant.now().toEpochMilli();
    }

    public static long currentTime() {
        return localtime();
    }

    public static long currentDate() {
        return LocalDate.now().toEpochDay();
    }

    public static long currentTimestamp() {
        return localtimestamp();
    }

    public static long now() {
        return localtimestamp();
    }

    public static int year(long date) {
        return LocalDate.ofEpochDay(date).getYear();
    }

    public static int quarter(long date) {
        return (LocalDate.ofEpochDay(date).getMonthValue() - 1) / 3 + 1;
    }

    public static int month(long date) {
        return LocalDate.ofEpochDay(date).getMonthValue();
    }

    public static int week(long date) {
        LocalDate localDate = LocalDate.ofEpochDay(date);
        WeekFields weekFields = WeekFields.ISO;
        return localDate.get(weekFields.weekOfWeekBasedYear());
    }

    public static String dateFormat(long date, String format) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        return dateFormat.format(new Date(date));
    }

    public static int toDate(String str) {
        return toDate(str, "yyyy-MM-dd");
    }

    public static int toDate(String str, String format) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        try {
            return dateFormat.parse(str).getDay();
        } catch (ParseException e) {
            LOG.error("Unsupported date type convert: {}", str);
            throw new RuntimeException(e);
        }
    }

    public static long toTimestamp(String str) {
        return toTimestamp(str, "yyyy-MM-dd HH:mm:ss");
    }

    public static long toTimestamp(String str, String format) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        try {
            return dateFormat.parse(str).getTime();
        } catch (ParseException e) {
            LOG.error("Unsupported date type convert: {}", str);
            throw new RuntimeException(e);
        }
    }

    public static long timestampDiff(String symbol, long fromDate, long toDate) {
        Calendar from = Calendar.getInstance();
        from.setTime(new Date(fromDate));
        Calendar to = Calendar.getInstance();
        to.setTime(new Date(toDate));
        long second = (to.getTimeInMillis() - from.getTimeInMillis()) / 1000;
        switch (symbol) {
            case "SECOND":
                return second;
            case "MINUTE":
                return second / 60;
            case "HOUR":
                return second / 3600;
            case "DAY":
                return second / (24 * 3600);
            case "MONTH":
                return to.get(Calendar.YEAR) * 12
                        + to.get(Calendar.MONDAY)
                        - (from.get(Calendar.YEAR) * 12 + from.get(Calendar.MONDAY));
            case "YEAR":
                return to.get(Calendar.YEAR) - from.get(Calendar.YEAR);
            default:
                LOG.error("Unsupported timestamp diff: {}", symbol);
                throw new RuntimeException("Unsupported timestamp diff: " + symbol);
        }
    }

    public static boolean betweenAsymmetric(String value, String minValue, String maxValue) {
        if (value == null) {
            return false;
        }
        return value.compareTo(minValue) >= 0 && value.compareTo(maxValue) <= 0;
    }

    public static boolean betweenAsymmetric(Short value, short minValue, short maxValue) {
        if (value == null) {
            return false;
        }
        return value >= minValue && value <= maxValue;
    }

    public static boolean betweenAsymmetric(Integer value, int minValue, int maxValue) {
        if (value == null) {
            return false;
        }
        return value >= minValue && value <= maxValue;
    }

    public static boolean betweenAsymmetric(Long value, long minValue, long maxValue) {
        if (value == null) {
            return false;
        }
        return value >= minValue && value <= maxValue;
    }

    public static boolean betweenAsymmetric(Float value, float minValue, float maxValue) {
        if (value == null) {
            return false;
        }
        return value >= minValue && value <= maxValue;
    }

    public static boolean betweenAsymmetric(Double value, double minValue, double maxValue) {
        if (value == null) {
            return false;
        }
        return value >= minValue && value <= maxValue;
    }

    public static boolean betweenAsymmetric(
            BigDecimal value, BigDecimal minValue, BigDecimal maxValue) {
        if (value == null) {
            return false;
        }
        return value.compareTo(minValue) >= 0 && value.compareTo(maxValue) <= 0;
    }

    public static boolean notBetweenAsymmetric(String value, String minValue, String maxValue) {
        return !betweenAsymmetric(value, minValue, maxValue);
    }

    public static boolean notBetweenAsymmetric(Short value, short minValue, short maxValue) {
        return !betweenAsymmetric(value, minValue, maxValue);
    }

    public static boolean notBetweenAsymmetric(Integer value, int minValue, int maxValue) {
        return !betweenAsymmetric(value, minValue, maxValue);
    }

    public static boolean notBetweenAsymmetric(Long value, long minValue, long maxValue) {
        return !betweenAsymmetric(value, minValue, maxValue);
    }

    public static boolean notBetweenAsymmetric(Float value, float minValue, float maxValue) {
        return !betweenAsymmetric(value, minValue, maxValue);
    }

    public static boolean notBetweenAsymmetric(Double value, double minValue, double maxValue) {
        return !betweenAsymmetric(value, minValue, maxValue);
    }

    public static boolean notBetweenAsymmetric(
            BigDecimal value, BigDecimal minValue, BigDecimal maxValue) {
        return !betweenAsymmetric(value, minValue, maxValue);
    }

    public static boolean in(String value, String... str) {
        return Arrays.stream(str).anyMatch(item -> value.equals(item));
    }

    public static boolean in(Short value, Short... values) {
        return Arrays.stream(values).anyMatch(item -> value.equals(item));
    }

    public static boolean in(Integer value, Integer... values) {
        return Arrays.stream(values).anyMatch(item -> value.equals(item));
    }

    public static boolean in(Long value, Long... values) {
        return Arrays.stream(values).anyMatch(item -> value.equals(item));
    }

    public static boolean in(Float value, Float... values) {
        return Arrays.stream(values).anyMatch(item -> value.equals(item));
    }

    public static boolean in(Double value, Double... values) {
        return Arrays.stream(values).anyMatch(item -> value.equals(item));
    }

    public static boolean in(BigDecimal value, BigDecimal... values) {
        return Arrays.stream(values).anyMatch(item -> value.equals(item));
    }

    public static boolean notIn(String value, String... values) {
        return !notIn(value, values);
    }

    public static boolean notIn(Short value, Short... values) {
        return !notIn(value, values);
    }

    public static boolean notIn(Integer value, Integer... values) {
        return !notIn(value, values);
    }

    public static boolean notIn(Long value, Long... values) {
        return !notIn(value, values);
    }

    public static boolean notIn(Float value, Float... values) {
        return !notIn(value, values);
    }

    public static boolean notIn(Double value, Double... values) {
        return !notIn(value, values);
    }

    public static boolean notIn(BigDecimal value, BigDecimal... values) {
        return !notIn(value, values);
    }

    public static int charLength(String str) {
        return str.length();
    }

    public static String trim(String symbol, String target, String str) {
        return str.trim();
    }

    /**
     * Returns a string resulting from replacing all substrings that match the regular expression
     * with replacement.
     */
    public static String regexpReplace(String str, String regex, String replacement) {
        if (str == null || regex == null || replacement == null) {
            return null;
        }
        try {
            return str.replaceAll(regex, Matcher.quoteReplacement(replacement));
        } catch (Exception e) {
            LOG.error(
                    String.format(
                            "Exception in regexpReplace('%s', '%s', '%s')",
                            str, regex, replacement),
                    e);
            // return null if exception in regex replace
            return null;
        }
    }

    public static String concat(String... str) {
        return String.join("", str);
    }

    public static boolean like(String str, String regex) {
        return Pattern.compile(regex).matcher(str).find();
    }

    public static boolean notLike(String str, String regex) {
        return !like(str, regex);
    }

    public static String substr(String str, int beginIndex) {
        return str.substring(beginIndex);
    }

    public static String substr(String str, int beginIndex, int endIndex) {
        return str.substring(beginIndex, endIndex);
    }

    public static String upper(String str) {
        return str.toUpperCase();
    }

    public static String lower(String str) {
        return str.toLowerCase();
    }

    /** SQL <code>ABS</code> operator applied to byte values. */
    public static byte abs(byte b0) {
        return (byte) Math.abs(b0);
    }

    /** SQL <code>ABS</code> operator applied to short values. */
    public static short abs(short b0) {
        return (short) Math.abs(b0);
    }

    /** SQL <code>ABS</code> operator applied to int values. */
    public static int abs(int b0) {
        return Math.abs(b0);
    }

    /** SQL <code>ABS</code> operator applied to long values. */
    public static long abs(long b0) {
        return Math.abs(b0);
    }

    /** SQL <code>ABS</code> operator applied to float values. */
    public static float abs(float b0) {
        return Math.abs(b0);
    }

    /** SQL <code>ABS</code> operator applied to double values. */
    public static double abs(double b0) {
        return Math.abs(b0);
    }

    public static double floor(double b0) {
        return Math.floor(b0);
    }

    public static float floor(float b0) {
        return (float) Math.floor(b0);
    }

    /** SQL <code>FLOOR</code> operator applied to int values. */
    public static int floor(int b0, int b1) {
        int r = b0 % b1;
        if (r < 0) {
            r += b1;
        }
        return b0 - r;
    }

    /** SQL <code>FLOOR</code> operator applied to long values. */
    public static long floor(long b0, long b1) {
        long r = b0 % b1;
        if (r < 0) {
            r += b1;
        }
        return b0 - r;
    }

    public static double ceil(double b0) {
        return Math.ceil(b0);
    }

    public static float ceil(float b0) {
        return (float) Math.ceil(b0);
    }

    /** SQL <code>CEIL</code> operator applied to int values. */
    public static int ceil(int b0, int b1) {
        int r = b0 % b1;
        if (r > 0) {
            r -= b1;
        }
        return b0 - r;
    }

    /** SQL <code>CEIL</code> operator applied to long values. */
    public static long ceil(long b0, long b1) {
        return floor(b0 + b1 - 1, b1);
    }

    // SQL ROUND
    /** SQL <code>ROUND</code> operator applied to byte values. */
    public static byte round(byte b0) {
        return round(b0, 0);
    }

    /** SQL <code>ROUND</code> operator applied to byte values. */
    public static byte round(byte b0, int b1) {
        return round(BigDecimal.valueOf(b0), b1).byteValue();
    }

    /** SQL <code>ROUND</code> operator applied to short values. */
    public static short round(short b0) {
        return round(b0, 0);
    }

    /** SQL <code>ROUND</code> operator applied to short values. */
    public static short round(short b0, int b1) {
        return round(BigDecimal.valueOf(b0), b1).shortValue();
    }

    /** SQL <code>ROUND</code> operator applied to int values. */
    public static int round(int b0) {
        return round(b0, 0);
    }

    /** SQL <code>ROUND</code> operator applied to int values. */
    public static int round(int b0, int b1) {
        return round(BigDecimal.valueOf(b0), b1).intValue();
    }

    /** SQL <code>ROUND</code> operator applied to long values. */
    public static long round(long b0) {
        return round(b0, 0);
    }

    /** SQL <code>ROUND</code> operator applied to long values. */
    public static long round(long b0, int b1) {
        return round(BigDecimal.valueOf(b0), b1).longValue();
    }

    /** SQL <code>ROUND</code> operator applied to BigDecimal values. */
    public static BigDecimal round(BigDecimal b0) {
        return round(b0, 0);
    }

    /** SQL <code>ROUND</code> operator applied to BigDecimal values. */
    public static BigDecimal round(BigDecimal b0, int b1) {
        return b0.movePointRight(b1).setScale(0, RoundingMode.HALF_UP).movePointLeft(b1);
    }

    /** SQL <code>ROUND</code> operator applied to float values. */
    public static float round(float b0) {
        return round(b0, 0);
    }

    /** SQL <code>ROUND</code> operator applied to float values. */
    public static float round(float b0, int b1) {
        return round(BigDecimal.valueOf(b0), b1).floatValue();
    }

    /** SQL <code>ROUND</code> operator applied to double values. */
    public static double round(double b0) {
        return round(b0, 0);
    }

    /** SQL <code>ROUND</code> operator applied to double values. */
    public static double round(double b0, int b1) {
        return round(BigDecimal.valueOf(b0), b1).doubleValue();
    }

    public static String uuid() {
        return UUID.randomUUID().toString();
    }

    public static String uuid(byte[] b) {
        return UUID.nameUUIDFromBytes(b).toString();
    }

    public static String nullif(String str1, String str2) {
        if (str1 == null && str2 == null) {
            return null;
        }
        if (str1 == null) {
            return str2;
        }
        if (str2 == null) {
            return str1;
        }
        return str1.equals(str2) ? null : str1;
    }

    public static Integer nullif(int numeric1, int numeric2) {
        return numeric1 == numeric2 ? null : numeric1;
    }

    public static Long nullif(long numeric1, long numeric2) {
        return numeric1 == numeric2 ? null : numeric1;
    }

    public static Float nullif(float numeric1, float numeric2) {
        return numeric1 == numeric2 ? null : numeric1;
    }

    public static Double nullif(double numeric1, double numeric2) {
        return numeric1 == numeric2 ? null : numeric1;
    }

    public static Short nullif(short numeric1, short numeric2) {
        return numeric1 == numeric2 ? null : numeric1;
    }

    public static Byte nullif(byte numeric1, byte numeric2) {
        return numeric1 == numeric2 ? null : numeric1;
    }

    public static BigDecimal nullif(BigDecimal numeric1, BigDecimal numeric2) {
        if (numeric1 == null && numeric2 == null) {
            return null;
        }
        if (numeric1 == null) {
            return numeric2;
        }
        if (numeric2 == null) {
            return numeric1;
        }
        return numeric1.equals(numeric2) ? null : numeric1;
    }

    public static Object coalesce(Object... objects) {
        for (Object item : objects) {
            if (item != null) {
                return item;
            }
        }
        return null;
    }
}
