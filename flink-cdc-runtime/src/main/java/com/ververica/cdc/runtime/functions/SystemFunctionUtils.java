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

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.DecimalDataUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.apache.flink.table.data.DecimalDataUtils.doubleValue;

/** System function utils to support the call of flink cdc pipeline transform. */
public class SystemFunctionUtils {
    private static final Logger LOG = LoggerFactory.getLogger(SystemFunctionUtils.class);

    // todo: Improve the comparison of various types.
    public static boolean betweenAsymmetric(Object value, Object minValue, Object maxValue) {
        if (value instanceof String) {
            String valueStr = (String) value;
            return valueStr.compareTo(String.valueOf(minValue)) >= 0
                    && valueStr.compareTo(String.valueOf(maxValue)) <= 0;
        } else if (value instanceof Integer) {
            Integer valueInt = (Integer) value;
            return valueInt >= Integer.parseInt(minValue.toString())
                    && valueInt >= Integer.parseInt(maxValue.toString());
        } else {
            LOG.error("Unsupported type comparison: {}", value.getClass());
            throw new RuntimeException("Unsupported type comparison: " + value.getClass());
        }
    }

    public static boolean notBetweenAsymmetric(Object value, Object minValue, Object maxValue) {
        return !betweenAsymmetric(value, minValue, maxValue);
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

    public static double power(double base, DecimalData exponent) {
        return Math.pow(base, doubleValue(exponent));
    }

    public static double power(DecimalData base, DecimalData exponent) {
        return Math.pow(doubleValue(base), doubleValue(exponent));
    }

    public static double power(DecimalData base, double exponent) {
        return Math.pow(doubleValue(base), exponent);
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

    public static DecimalData abs(DecimalData a) {
        return DecimalDataUtils.abs(a);
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

    public static DecimalData floor(DecimalData a) {
        return DecimalDataUtils.floor(a);
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

    public static DecimalData ceil(DecimalData a) {
        return DecimalDataUtils.ceil(a);
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

    /** SQL <code>ROUND</code> operator applied to DecimalData values. */
    public static DecimalData round(DecimalData b0) {
        return round(b0, 0);
    }

    /** SQL <code>ROUND</code> operator applied to DecimalData values. */
    public static DecimalData round(DecimalData b0, int b1) {
        return DecimalDataUtils.sround(b0, b1);
    }

    public static String uuid() {
        return UUID.randomUUID().toString();
    }

    public static String uuid(byte[] b) {
        return UUID.nameUUIDFromBytes(b).toString();
    }
}
