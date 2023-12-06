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
package com.zavtech.morpheus.array;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Currency;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Units tests for the Array type
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class ArraysBasicTests {

    private static Currency[] currencies = Currency.getAvailableCurrencies().stream().toArray(Currency[]::new);

    private static Class[] classes = new Class[] {
        Object.class,
        Boolean.class,
        Integer.class,
        Long.class,
        Double.class,
        String.class,
        Month.class,
        Currency.class,
        Date.class,
        LocalDate.class,
        LocalTime.class,
        LocalDateTime.class,
        ZonedDateTime.class
    };


    @DataProvider(name="SparseOrDense")
    public Object[][] getSparseOrDense() {
        return new Object[][] {
                { false },
                { true }
        };
    }

    @DataProvider(name="classes")
    public Object[][] getClasses() {
        final Object[][] args = new Object[classes.length][];
        for (int i=0; i<classes.length; ++i) {
            args[i] = new Object[] { classes[i] };
        }
        return args;
    }


    @DataProvider(name = "types")
    public Object[][] types() {
        final List<Object[]> argList = new ArrayList<>();
        for (ArrayStyle style : ArrayStyle.values()) {
            for (Class<?> clazz : classes) {
                if (style.isMapped()) {
                    final ArrayType type = ArrayType.of(clazz);
                    if (!type.isString() && !type.isObject()) {
                        argList.add(new Object[]  { clazz, style });
                    }
                } else {
                    argList.add(new Object[]  { clazz, style });
                }
            }
        }
        return argList.toArray(new Object[argList.size()][]);
    }

    @Test()
    public void testArrayType() {
        Assert.assertEquals(Array.of(true, false, true, false).getClass().getSimpleName(), "DenseArrayOfBooleans");
        Assert.assertEquals(Array.of(new boolean[] {true, false, true, false}).getClass().getSimpleName(), "DenseArrayOfBooleans");
        Assert.assertEquals(Array.of(1, 2, 3, 4, 5).getClass().getSimpleName(), "DenseArrayOfInts");
        Assert.assertEquals(Array.of(new int[] {1, 2, 3, 4, 5}).getClass().getSimpleName(), "DenseArrayOfInts");
        Assert.assertEquals(Array.of(1L, 2L, 3L, 4L, 5L).getClass().getSimpleName(), "DenseArrayOfLongs");
        Assert.assertEquals(Array.of(new long[] {1L, 2L, 3L, 4L, 5L}).getClass().getSimpleName(), "DenseArrayOfLongs");
        Assert.assertEquals(Array.of(1d, 2d, 3d, 4d, 5d).getClass().getSimpleName(), "DenseArrayOfDoubles");
        Assert.assertEquals(Array.of(new double[] {1d, 2d, 3d, 4d, 5d}).getClass().getSimpleName(), "DenseArrayOfDoubles");
        Assert.assertEquals(Array.of("Hello", LocalDate.of(1998, 1, 1), Month.JANUARY, 56.45d, true).getClass().getSimpleName(), "DenseArrayOfObjects");
        Assert.assertEquals(Array.of(new Object[] {"Hello", LocalDate.of(1998, 1, 1), Month.JANUARY, 56.45d, true}).getClass().getSimpleName(), "DenseArrayOfObjects");
        Assert.assertEquals(Array.of(new Object[] {"Hello", LocalDate.of(1998, 1, 1), Month.JANUARY, 56.45d, true}).length(), 5);
    }



    @Test()
    public void testDenseConstruction() {
        Assert.assertEquals(Array.of(Boolean.class, 20).typeCode(), ArrayType.BOOLEAN);
        Assert.assertEquals(Array.of(Integer.class, 20).typeCode(), ArrayType.INTEGER);
        Assert.assertEquals(Array.of(Long.class, 20).typeCode(), ArrayType.LONG);
        Assert.assertEquals(Array.of(Double.class, 20).typeCode(), ArrayType.DOUBLE);
        Assert.assertEquals(Array.of(String.class, 20).typeCode(), ArrayType.STRING);
        Assert.assertEquals(Array.of(Date.class, 20).typeCode(), ArrayType.DATE);
        Assert.assertEquals(Array.of(LocalDate.class, 20).typeCode(), ArrayType.LOCAL_DATE);
        Assert.assertEquals(Array.of(LocalTime.class, 20).typeCode(), ArrayType.LOCAL_TIME);
        Assert.assertEquals(Array.of(LocalDateTime.class, 20).typeCode(), ArrayType.LOCAL_DATETIME);
        Assert.assertEquals(Array.of(ZonedDateTime.class, 20).typeCode(), ArrayType.ZONED_DATETIME);
        Assert.assertEquals(Array.of(Object.class, 20).typeCode(), ArrayType.OBJECT);

        Assert.assertEquals(Array.of(Boolean.class, 20).length(), 20);
        Assert.assertEquals(Array.of(Integer.class, 20).length(), 20);
        Assert.assertEquals(Array.of(Long.class, 20).length(), 20);
        Assert.assertEquals(Array.of(Double.class, 20).length(), 20);
        Assert.assertEquals(Array.of(String.class, 20).length(), 20);
        Assert.assertEquals(Array.of(Date.class, 20).length(), 20);
        Assert.assertEquals(Array.of(LocalDate.class, 20).length(), 20);
        Assert.assertEquals(Array.of(LocalTime.class, 20).length(), 20);
        Assert.assertEquals(Array.of(LocalDateTime.class, 20).length(), 20);
        Assert.assertEquals(Array.of(ZonedDateTime.class, 20).length(), 20);
        Assert.assertEquals(Array.of(Object.class, 20).length(), 20);

        Assert.assertEquals(Array.ofIterable(Arrays.asList(true, false, true)).typeCode(), ArrayType.BOOLEAN);
        Assert.assertEquals(Array.ofIterable(Arrays.asList(1, 2, 3, 4)).typeCode(), ArrayType.INTEGER);
        Assert.assertEquals(Array.ofIterable(Arrays.asList(1L, 2L, 3L, 4L)).typeCode(), ArrayType.LONG);
        Assert.assertEquals(Array.ofIterable(Arrays.asList(1d, 2d, 3d, 4d)).typeCode(), ArrayType.DOUBLE);
        Assert.assertEquals(Array.ofIterable(Arrays.asList("1d", "2d", "3d", "4d")).typeCode(), ArrayType.STRING);
        Assert.assertEquals(Array.ofIterable(Arrays.asList(1d, "2d", false, 4d)).typeCode(), ArrayType.OBJECT);

        Assert.assertEquals(Array.ofIterable(Arrays.asList(true, false, true)).length(), 3);
        Assert.assertEquals(Array.ofIterable(Arrays.asList(1, 2, 3, 4)).length(), 4);
        Assert.assertEquals(Array.ofIterable(Arrays.asList(1L, 2L, 3L, 4L)).length(), 4);
        Assert.assertEquals(Array.ofIterable(Arrays.asList(1d, 2d, 3d, 4d)).length(), 4);
        Assert.assertEquals(Array.ofIterable(Arrays.asList("1d", "2d", "3d", "4d")).length(), 4);
        Assert.assertEquals(Array.ofIterable(Arrays.asList(1d, "2d", false, 4L, 5)).length(), 5);
    }


    @Test()
    public void testSparseConstruction() {
        Assert.assertTrue(Array.of(Integer.class, 20, 0.5F).style().isSparse());
        Assert.assertTrue(Array.of(Long.class, 20, 0.5F).style().isSparse());
        Assert.assertTrue(Array.of(Double.class, 20, 0.5F).style().isSparse());
        Assert.assertTrue(Array.of(String.class, 20, 0.5F).style().isSparse());
        Assert.assertTrue(Array.of(Date.class, 20, 0.5F).style().isSparse());
        Assert.assertTrue(Array.of(LocalDate.class, 20, 0.5F).style().isSparse());
        Assert.assertTrue(Array.of(LocalTime.class, 20, 0.5F).style().isSparse());
        Assert.assertTrue(Array.of(LocalDateTime.class, 20, 0.5F).style().isSparse());
        Assert.assertTrue(Array.of(ZonedDateTime.class, 20, 0.5F).style().isSparse());

        Assert.assertEquals(Array.of(Integer.class, 20, 0.5F).length(), 20);
        Assert.assertEquals(Array.of(Long.class, 20, 0.5F).length(), 20);
        Assert.assertEquals(Array.of(Double.class, 20, 0.5F).length(), 20);
        Assert.assertEquals(Array.of(String.class, 20, 0.5F).length(), 20);
        Assert.assertEquals(Array.of(Date.class, 20, 0.5F).length(), 20);
        Assert.assertEquals(Array.of(LocalDate.class, 20, 0.5F).length(), 20);
        Assert.assertEquals(Array.of(LocalTime.class, 20, 0.5F).length(), 20);
        Assert.assertEquals(Array.of(LocalDateTime.class, 20, 0.5F).length(), 20);
        Assert.assertEquals(Array.of(ZonedDateTime.class, 20, 0.5F).length(), 20);
    }


    @Test(dataProvider = "SparseOrDense")
    public void testToString(boolean sparse) {
        final float loadFactor = sparse ? 0.5f : 1f;
        Assert.assertTrue(Array.of(Boolean.class, 20, loadFactor).toString() != null);
        Assert.assertTrue(Array.of(Integer.class, 20, loadFactor).toString() != null);
        Assert.assertTrue(Array.of(Long.class, 20, loadFactor).toString() != null);
        Assert.assertTrue(Array.of(Double.class, 20, loadFactor).toString() != null);
        Assert.assertTrue(Array.of(String.class, 20, loadFactor).toString() != null);
        Assert.assertTrue(Array.of(Date.class, 20, loadFactor).toString() != null);
        Assert.assertTrue(Array.of(LocalDate.class, 20, loadFactor).toString() != null);
        Assert.assertTrue(Array.of(LocalTime.class, 20, loadFactor).toString() != null);
        Assert.assertTrue(Array.of(LocalDateTime.class, 20, loadFactor).toString() != null);
        Assert.assertTrue(Array.of(ZonedDateTime.class, 20, loadFactor).toString() != null);
    }



    @Test(dataProvider = "SparseOrDense")
    public void testDefaultValues(boolean sparse) {
        final Date date = new Date();
        final float loadFactor = sparse ? 0.5F : 1F;
        final LocalDate ld = LocalDate.of(1974, 11, 8);
        final LocalTime lt = LocalTime.of(18, 35);
        final LocalDateTime ldt = LocalDateTime.of(2000, 1, 1, 4, 5);
        final ZonedDateTime zdt = ZonedDateTime.of(2010, 1, 4, 2, 6, 0, 0, ZoneId.of("Europe/London"));

        Array.of(Boolean.class, 100, true, loadFactor).forEachValue(v -> Assert.assertEquals(v.getBoolean(), true));
        Array.of(Integer.class, 100, 35, loadFactor).forEachValue(v -> Assert.assertEquals(v.getInt(), 35));
        Array.of(Long.class, 100, 75L, loadFactor).forEachValue(v -> Assert.assertEquals(v.getLong(), 75L));
        Array.of(Double.class, 100, 0.45d, loadFactor).forEachValue(v -> Assert.assertEquals(v.getDouble(), 0.45d));
        Array.of(String.class, 100, "Hello", loadFactor).forEachValue(v -> Assert.assertEquals(v.getValue(), "Hello"));
        Array.of(Date.class, 100, date, loadFactor).forEachValue(v -> Assert.assertEquals(v.getValue(), date));
        Array.of(LocalDate.class, 100, ld, loadFactor).forEachValue(v -> Assert.assertEquals(v.getValue(), ld));
        Array.of(LocalTime.class, 100, lt, loadFactor).forEachValue(v -> Assert.assertEquals(v.getValue(), lt));
        Array.of(LocalDateTime.class, 100, ldt, loadFactor).forEachValue(v -> Assert.assertEquals(v.getValue(), ldt));
        Array.of(ZonedDateTime.class, 100, zdt, loadFactor).forEachValue(v -> Assert.assertEquals(v.getValue(), zdt));

        Array.ofObjects(100, true, loadFactor).forEachValue(v -> Assert.assertEquals(v.getBoolean(), true));
        Array.ofObjects(100, 35, loadFactor).forEachValue(v -> Assert.assertEquals(v.getInt(), 35));
        Array.ofObjects(100, 75L, loadFactor).forEachValue(v -> Assert.assertEquals(v.getLong(), 75L));
        Array.ofObjects(100, 0.45d, loadFactor).forEachValue(v -> Assert.assertEquals(v.getDouble(), 0.45d));
        Array.ofObjects(100, "Hello", loadFactor).forEachValue(v -> Assert.assertEquals(v.getValue(), "Hello"));
        Array.ofObjects(100, date, loadFactor).forEachValue(v -> Assert.assertEquals(v.getValue(), date));
        Array.ofObjects(100, ld, loadFactor).forEachValue(v -> Assert.assertEquals(v.getValue(), ld));
        Array.ofObjects(100, lt, loadFactor).forEachValue(v -> Assert.assertEquals(v.getValue(), lt));
        Array.ofObjects(100, ldt, loadFactor).forEachValue(v -> Assert.assertEquals(v.getValue(), ldt));
        Array.ofObjects(100, zdt, loadFactor).forEachValue(v -> Assert.assertEquals(v.getValue(), zdt));

        Array.of(Boolean.class, 100, true, loadFactor).expand(1000).forEachValue(v -> Assert.assertEquals(v.getBoolean(), true));
        Array.of(Integer.class, 100, 35, loadFactor).expand(1000).forEachValue(v -> Assert.assertEquals(v.getInt(), 35));
        Array.of(Long.class, 100, 75L, loadFactor).expand(1000).forEachValue(v -> Assert.assertEquals(v.getLong(), 75L));
        Array.of(Double.class, 100, 0.45d, loadFactor).expand(1000).forEachValue(v -> Assert.assertEquals(v.getDouble(), 0.45d));
        Array.of(String.class, 100, "Hello", loadFactor).expand(1000).forEachValue(v -> Assert.assertEquals(v.getValue(), "Hello", "At index = " + v.index()));
        Array.of(Date.class, 100, date, loadFactor).expand(1000).forEachValue(v -> Assert.assertEquals(v.getValue(), date));
        Array.of(LocalDate.class, 100, ld, loadFactor).expand(1000).forEachValue(v -> Assert.assertEquals(v.getValue(), ld));
        Array.of(LocalTime.class, 100, lt, loadFactor).expand(1000).forEachValue(v -> Assert.assertEquals(v.getValue(), lt));
        Array.of(LocalDateTime.class, 100, ldt, loadFactor).expand(1000).forEachValue(v -> Assert.assertEquals(v.getValue(), ldt));
        Array.of(ZonedDateTime.class, 100, zdt, loadFactor).expand(1000).forEachValue(v -> Assert.assertEquals(v.getValue(), zdt));

        Array.ofObjects(100, true, loadFactor).expand(1000).forEachValue(v -> Assert.assertEquals(v.getBoolean(), true));
        Array.ofObjects(100, 35, loadFactor).expand(1000).forEachValue(v -> Assert.assertEquals(v.getInt(), 35));
        Array.ofObjects(100, 75L, loadFactor).expand(1000).forEachValue(v -> Assert.assertEquals(v.getLong(), 75L));
        Array.ofObjects(100, 0.45d, loadFactor).expand(1000).forEachValue(v -> Assert.assertEquals(v.getDouble(), 0.45d));
        Array.ofObjects(100, "Hello", loadFactor).expand(1000).forEachValue(v -> Assert.assertEquals(v.getValue(), "Hello"));
        Array.ofObjects(100, date, loadFactor).expand(1000).forEachValue(v -> Assert.assertEquals(v.getValue(), date));
        Array.ofObjects(100, ld, loadFactor).expand(1000).forEachValue(v -> Assert.assertEquals(v.getValue(), ld));
        Array.ofObjects(100, lt, loadFactor).expand(1000).forEachValue(v -> Assert.assertEquals(v.getValue(), lt));
        Array.ofObjects(100, ldt, loadFactor).expand(1000).forEachValue(v -> Assert.assertEquals(v.getValue(), ldt));
        Array.ofObjects(100, zdt, loadFactor).expand(1000).forEachValue(v -> Assert.assertEquals(v.getValue(), zdt));
    }


    @Test(dataProvider = "types", expectedExceptions = {ArrayIndexOutOfBoundsException.class})
    public <T> void testIndexOutOfLowerBounds(Class<T> type, ArrayStyle style) {
        final Array<T> array = Array.of(type, 100, ArrayType.defaultValue(type), style);
        if (type != Boolean.class) Assert.assertEquals(array.style(), style);
        array.getValue(-5);
    }


    @Test(dataProvider = "types", expectedExceptions = {ArrayIndexOutOfBoundsException.class})
    public <T> void testIndexOutOfUpperBounds(Class<T> type, ArrayStyle style) {
        final Array<T> array = Array.of(type, 100, ArrayType.defaultValue(type), style);
        if (type != Boolean.class) Assert.assertEquals(array.style(), style);
        array.getValue(200);
    }


    @Test(dataProvider = "SparseOrDense")
    public void testBooleanArray(boolean sparse) {
        final Random random = new Random();
        final boolean[] values = new boolean[100];
        final float loadFactor = sparse ? 0.5F : 1F;
        final Array<Boolean> array = Array.of(Boolean.class, values.length, loadFactor);
        Assert.assertEquals(array.typeCode(), ArrayType.BOOLEAN, "Array type matches");
        for (int i=0; i<values.length; ++i) {
            Assert.assertTrue(!array.getBoolean(i), "Matches null value");
        }
        for (int i=0; i<values.length; ++i) {
            final boolean value = random.nextBoolean();
            values[i] = value;
            array.setBoolean(i, value);
        }
        for (int i=0; i<values.length; ++i) {
            final boolean v1 = values[i];
            final boolean v2 = array.getBoolean(i);
            final Boolean v3 = array.getValue(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
            Assert.assertEquals(v2, v3.booleanValue(), "Values match at " + i);
        }
        array.expand(200);
        Assert.assertEquals(array.length(), 200, "The array was expanded");
        for (int i=0; i<values.length; ++i) {
            final boolean v1 = values[i];
            final boolean v2 = array.getBoolean(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
        }
        for (int i=values.length; i<200; ++i) {
            Assert.assertTrue(!array.getBoolean(i), "Matches null value at " + i);
        }
        for (int i=100; i<200; ++i) {
            final boolean value = random.nextBoolean();
            values[i-100] = value;
            array.setValue(i, value);
        }
        for (int i=100; i<200; ++i) {
            final boolean v1 = values[i-100];
            final boolean v2 = array.getBoolean(i);
            final Boolean v3 = array.getValue(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
            Assert.assertEquals(v2, v3.booleanValue(), "Values match at " + i);
        }
    }


    @Test(dataProvider = "SparseOrDense")
    public void testIntArray(boolean sparse) {
        final Random random = new Random();
        final int[] values = new int[100];
        final float loadFactor = sparse ? 0.5F : 1F;
        final Array<Integer> array = Array.of(Integer.class, values.length, loadFactor);
        for (int i=0; i<values.length; ++i) {
            Assert.assertTrue(array.getInt(i) == 0, "Matches null value");
        }
        for (int i=0; i<values.length; ++i) {
            final int value = random.nextInt();
            values[i] = value;
            array.setInt(i, value);
        }
        for (int i=0; i<values.length; ++i) {
            final int v1 = values[i];
            final int v2 = array.getInt(i);
            final Integer v3 = array.getValue(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
            Assert.assertEquals(v2, v3.intValue(), "Values match at " + i);
        }
        array.expand(200);
        Assert.assertEquals(array.length(), 200, "The array was expanded");
        for (int i=0; i<values.length; ++i) {
            final int v1 = values[i];
            final int v2 = array.getInt(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
        }
        for (int i=values.length; i<200; ++i) {
            Assert.assertTrue(array.getInt(i) == 0, "Matches null value at " + i);
        }
        for (int i=100; i<200; ++i) {
            final int value = random.nextInt();
            values[i-100] = value;
            array.setValue(i, value);
        }
        for (int i=100; i<200; ++i) {
            final int v1 = values[i-100];
            final int v2 = array.getInt(i);
            final Integer v3 = array.getValue(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
            Assert.assertEquals(v2, v3.intValue(), "Values match at " + i);
        }
    }


    @Test(dataProvider = "SparseOrDense")
    public void testLongArray(boolean sparse) {
        final long nullValue = 0L;
        final Random random = new Random();
        final long[] values = new long[100];
        final float loadFactor = sparse ? 0.5F : 1F;
        final Array<Long> array = Array.of(Long.class, values.length, loadFactor);
        for (int i=0; i<values.length; ++i) {
            Assert.assertTrue(array.getLong(i) == nullValue, "Matches null value");
        }
        for (int i=0; i<values.length; ++i) {
            final long value = random.nextLong();
            values[i] = value;
            array.setLong(i, value);
        }
        for (int i=0; i<values.length; ++i) {
            final long v1 = values[i];
            final long v2 = array.getLong(i);
            final Long v3 = array.getValue(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
            Assert.assertEquals(v2, v3.longValue(), "Values match at " + i);
        }
        array.expand(200);
        Assert.assertEquals(array.length(), 200, "The array was expanded");
        for (int i=0; i<values.length; ++i) {
            final long v1 = values[i];
            final long v2 = array.getLong(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
        }
        for (int i=values.length; i<200; ++i) {
            Assert.assertTrue(array.getLong(i) == 0, "Matches null value at " + i);
        }
        for (int i=100; i<200; ++i) {
            final long value = random.nextLong();
            values[i-100] = value;
            array.setValue(i, value);
        }
        for (int i=100; i<200; ++i) {
            final long v1 = values[i-100];
            final long v2 = array.getLong(i);
            final Long v3 = array.getValue(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
            Assert.assertEquals(v2, v3.longValue(), "Values match at " + i);
        }
    }


    @Test(dataProvider = "SparseOrDense")
    public void testDoubleArray(boolean sparse) {
        final Random random = new Random();
        final double[] values = new double[100];
        final float loadFactor = sparse ? 0.5F : 1F;
        final Array<Double> array = Array.of(Double.class, values.length, loadFactor);
        for (int i=0; i<values.length; ++i) {
            Assert.assertTrue(Double.isNaN(array.getDouble(i)), "Matches null value");
        }
        for (int i=0; i<values.length; ++i) {
            final double value = random.nextDouble();
            values[i] = value;
            array.setDouble(i, value);
        }
        for (int i=0; i<values.length; ++i) {
            final double v1 = values[i];
            final double v2 = array.getDouble(i);
            final Double v3 = array.getValue(i);
            Assert.assertEquals(v1, v2, 0.00000000001, "Values match at " + i);
            Assert.assertEquals(v2, v3, 0.00000000001, "Values match at " + i);
        }
        array.expand(200);
        Assert.assertEquals(array.length(), 200, "The array was expanded");
        for (int i=0; i<values.length; ++i) {
            final double v1 = values[i];
            final double v2 = array.getDouble(i);
            Assert.assertEquals(v1, v2, 0.00000000001, "Values match at " + i);
        }
        for (int i=values.length; i<200; ++i) {
            Assert.assertTrue(Double.isNaN(array.getDouble(i)), "Matches null value at " + i);
        }
        for (int i=100; i<200; ++i) {
            final double value = random.nextDouble();
            values[i-100] = value;
            array.setValue(i, value);
        }
        for (int i=100; i<200; ++i) {
            final double v1 = values[i-100];
            final double v2 = array.getDouble(i);
            final Double v3 = array.getValue(i);
            Assert.assertEquals(v1, v2, 0.00000000001, "Values match at " + i);
            Assert.assertEquals(v2, v3, 0.00000000001, "Values match at " + i);
        }
    }


    @Test(dataProvider = "SparseOrDense")
    public void testStringArray(boolean sparse) {
        final String nullValue = null;
        final Random random = new Random();
        final String[] values = new String[100];
        final float loadFactor = sparse ? 0.5F : 1F;
        final Array<String> array = Array.of(String.class, values.length, loadFactor);
        for (int i=0; i<values.length; ++i) {
            Assert.assertEquals(array.getValue(i), nullValue, "Matches null value");
        }
        for (int i=0; i<values.length; ++i) {
            final String value = "X:" + random.nextDouble();
            values[i] = value;
            array.setValue(i, value);
        }
        for (int i=0; i<values.length; ++i) {
            final String v1 = values[i];
            final String v2 = array.getValue(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
        }
        array.expand(200);
        Assert.assertEquals(array.length(), 200, "The array was expanded");
        for (int i=0; i<values.length; ++i) {
            final String v1 = values[i];
            final String v2 = array.getValue(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
        }
        for (int i=values.length; i<200; ++i) {
            Assert.assertEquals(nullValue, array.getValue(i), "Matches null value at " + i);
        }
        for (int i=100; i<200; ++i) {
            final String value = "X:" + random.nextLong();
            values[i-100] = value;
            array.setValue(i, value);
        }
        for (int i=100; i<200; ++i) {
            final String v1 = values[i-100];
            final String v2 = array.getValue(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
        }
    }


    @Test(dataProvider = "SparseOrDense")
    public void testLocalDateArray(boolean sparse) {
        final LocalDate nullValue = null;
        final LocalDate[] values = new LocalDate[100];
        final float loadFactor = sparse ? 0.5F : 1F;
        final Array<LocalDate> array = Array.of(LocalDate.class, values.length, loadFactor);
        for (int i=0; i<values.length; ++i) {
            Assert.assertEquals(array.getValue(i), nullValue, "Matches null value");
        }
        for (int i=0; i<values.length; ++i) {
            final int dayCount = (int)(Math.random() * 100);
            final LocalDate value = LocalDate.now().minusDays(dayCount);
            values[i] = value;
            array.setValue(i, value);
        }
        for (int i=0; i<values.length; ++i) {
            final LocalDate v1 = values[i];
            final LocalDate v2 = array.getValue(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
        }
        array.expand(200);
        Assert.assertEquals(array.length(), 200, "The array was expanded");
        for (int i=0; i<values.length; ++i) {
            final LocalDate v1 = values[i];
            final LocalDate v2 = array.getValue(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
        }
        for (int i=values.length; i<200; ++i) {
            Assert.assertEquals(nullValue, array.getValue(i), "Matches null value at " + i);
        }
        for (int i=100; i<200; ++i) {
            final int dayCount = (int)(Math.random() * 100);
            final LocalDate value = LocalDate.now().minusDays(dayCount);
            values[i-100] = value;
            array.setValue(i, value);
        }
        for (int i=100; i<200; ++i) {
            final LocalDate v1 = values[i-100];
            final LocalDate v2 = array.getValue(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
        }
    }


    @Test(dataProvider = "SparseOrDense")
    public void testLocalTimeArray(boolean sparse) {
        final LocalTime nullValue = null;
        final LocalTime[] values = new LocalTime[100];
        final float loadFactor = sparse ? 0.5F : 1F;
        final Array<LocalTime> array = Array.of(LocalTime.class, values.length, loadFactor);
        for (int i=0; i<values.length; ++i) {
            Assert.assertEquals(array.getValue(i), nullValue, "Matches null value");
        }
        for (int i=0; i<values.length; ++i) {
            final int minCount = (int)(Math.random() * 100);
            final LocalTime value = LocalTime.now().minusMinutes(minCount);
            values[i] = value;
            array.setValue(i, value);
        }
        for (int i=0; i<values.length; ++i) {
            final LocalTime v1 = values[i];
            final LocalTime v2 = array.getValue(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
        }
        array.expand(200);
        Assert.assertEquals(array.length(), 200, "The array was expanded");
        for (int i=0; i<values.length; ++i) {
            final LocalTime v1 = values[i];
            final LocalTime v2 = array.getValue(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
        }
        for (int i=values.length; i<200; ++i) {
            Assert.assertEquals(nullValue, array.getValue(i), "Matches null value at " + i);
        }
        for (int i=100; i<200; ++i) {
            final int minCount = (int)(Math.random() * 100);
            final LocalTime value = LocalTime.now().minusMinutes(minCount);
            values[i-100] = value;
            array.setValue(i, value);
        }
        for (int i=100; i<200; ++i) {
            final LocalTime v1 = values[i-100];
            final LocalTime v2 = array.getValue(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
        }
    }


    @Test(dataProvider = "SparseOrDense")
    public void testLocalDateTimeArray(boolean sparse) {
        final LocalDateTime nullValue = null;
        final LocalDateTime[] values = new LocalDateTime[100];
        final float loadFactor = sparse ? 0.5F : 1F;
        final Array<LocalDateTime> array = Array.of(LocalDateTime.class, values.length, loadFactor);
        for (int i=0; i<values.length; ++i) {
            Assert.assertEquals(array.getValue(i), nullValue, "Matches null value");
        }
        for (int i=0; i<values.length; ++i) {
            final int minCount = (int)(Math.random() * 100);
            final LocalDateTime value = LocalDateTime.now().minusMinutes(minCount);
            values[i] = value;
            array.setValue(i, value);
        }
        for (int i=0; i<values.length; ++i) {
            final LocalDateTime v1 = values[i].truncatedTo(ChronoUnit.MILLIS);
            final LocalDateTime v2 = array.getValue(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
        }
        array.expand(200);
        Assert.assertEquals(array.length(), 200, "The array was expanded");
        for (int i=0; i<values.length; ++i) {
            final LocalDateTime v1 = values[i].truncatedTo(ChronoUnit.MILLIS);
            final LocalDateTime v2 = array.getValue(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
        }
        for (int i=values.length; i<200; ++i) {
            Assert.assertEquals(nullValue, array.getValue(i), "Matches null value at " + i);
        }
        for (int i=100; i<200; ++i) {
            final int minCount = (int)(Math.random() * 100);
            final LocalDateTime value = LocalDateTime.now().minusMinutes(minCount);
            values[i-100] = value;
            array.setValue(i, value);
        }
        for (int i=100; i<200; ++i) {
            final LocalDateTime v1 = values[i-100].truncatedTo(ChronoUnit.MILLIS);
            final LocalDateTime v2 = array.getValue(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
        }
    }


    @Test(dataProvider = "SparseOrDense")
    public void testZonedDateTimeArray(boolean sparse) {
        final ZonedDateTime nullValue = null;
        final ZonedDateTime[] values = new ZonedDateTime[100];
        final float loadFactor = sparse ? 0.5F : 1F;
        final String[] zoneIds = ZoneId.getAvailableZoneIds().stream().toArray(String[]::new);
        final Array<ZonedDateTime> array = Array.of(ZonedDateTime.class, values.length, loadFactor);
        for (int i=0; i<values.length; ++i) {
            Assert.assertEquals(array.getValue(i), nullValue, "Matches null value");
        }
        for (int i=0; i<values.length; ++i) {
            final int minCount = (int)(Math.random() * 100);
            final ZoneId zoneId = ZoneId.of(zoneIds[(int)(Math.random() * zoneIds.length)]);
            final ZonedDateTime value = ZonedDateTime.now(zoneId).minusMinutes(minCount);
            values[i] = value;
            array.setValue(i, value);
        }
        for (int i=0; i<values.length; ++i) {
            final ZonedDateTime v1 = values[i].truncatedTo(ChronoUnit.MILLIS);
            final ZonedDateTime v2 = array.getValue(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
        }
        array.expand(200);
        Assert.assertEquals(array.length(), 200, "The array was expanded");
        for (int i=0; i<values.length; ++i) {
            final ZonedDateTime v1 = values[i].truncatedTo(ChronoUnit.MILLIS);
            final ZonedDateTime v2 = array.getValue(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
        }
        for (int i=values.length; i<200; ++i) {
            Assert.assertEquals(nullValue, array.getValue(i), "Matches null value at " + i);
        }
        for (int i=100; i<200; ++i) {
            final int minCount = (int)(Math.random() * 100);
            final ZoneId zoneId = ZoneId.of(zoneIds[(int)(Math.random() * zoneIds.length)]);
            final ZonedDateTime value = ZonedDateTime.now(zoneId).minusMinutes(minCount);
            values[i-100] = value;
            array.setValue(i, value);
        }
        for (int i=100; i<200; ++i) {
            final ZonedDateTime v1 = values[i-100].truncatedTo(ChronoUnit.MILLIS);
            final ZonedDateTime v2 = array.getValue(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
        }
    }


    @Test(dataProvider = "SparseOrDense")
    public void testAnyArray(boolean sparse) {
        final ZonedDateTime nullValue = null;
        final ZonedDateTime[] values = new ZonedDateTime[100];
        final float loadFactor = sparse ? 0.5F : 1F;
        final String[] zoneIds = ZoneId.getAvailableZoneIds().stream().toArray(String[]::new);
        final Array<ZonedDateTime> array = Array.ofObjects(values.length, loadFactor);
        for (int i=0; i<values.length; ++i) {
            Assert.assertEquals(array.getValue(i), nullValue, "Matches null value");
        }
        for (int i=0; i<values.length; ++i) {
            final int minCount = (int)(Math.random() * 100);
            final ZoneId zoneId = ZoneId.of(zoneIds[(int)(Math.random() * zoneIds.length)]);
            final ZonedDateTime value = ZonedDateTime.now(zoneId).minusMinutes(minCount);
            values[i] = value;
            array.setValue(i, value);
        }
        for (int i=0; i<values.length; ++i) {
            final ZonedDateTime v1 = values[i];
            final ZonedDateTime v2 = array.getValue(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
        }
        array.expand(200);
        Assert.assertEquals(array.length(), 200, "The array was expanded");
        for (int i=0; i<values.length; ++i) {
            final ZonedDateTime v1 = values[i];
            final ZonedDateTime v2 = array.getValue(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
        }
        for (int i=values.length; i<200; ++i) {
            Assert.assertEquals(nullValue, array.getValue(i), "Matches null value at " + i);
        }
        for (int i=100; i<200; ++i) {
            final int minCount = (int)(Math.random() * 100);
            final ZoneId zoneId = ZoneId.of(zoneIds[(int)(Math.random() * zoneIds.length)]);
            final ZonedDateTime value = ZonedDateTime.now(zoneId).minusMinutes(minCount);
            values[i-100] = value;
            array.setValue(i, value);
        }
        for (int i=100; i<200; ++i) {
            final ZonedDateTime v1 = values[i-100];
            final ZonedDateTime v2 = array.getValue(i);
            Assert.assertEquals(v1, v2, "Values match at " + i);
        }
    }

    @Test
    public void testArrayWrappers() {
        final int size = 1000;
        final Random random = new Random();
        final boolean[] booleans = new boolean[size];
        final int[] integers = new int[size];
        final long[] longs = new long[size];
        final double[] doubles = new double[size];
        final LocalDate[] localDates = new LocalDate[size];

        for (int i=0; i<size; ++i) {
            booleans[i] = random.nextBoolean();
            integers[i] = random.nextInt();
            longs[i] = random.nextLong();
            doubles[i] = random.nextDouble();
            localDates[i] = LocalDate.now().minusDays(i);
        }

        final Array<Boolean> booleanArray = Array.of(booleans);
        final Array<Integer> integerArray = Array.of(integers);
        final Array<Long> longArray = Array.of(longs);
        final Array<Double> doubleArray = Array.of(doubles);
        final Array<LocalDate> objectArray = Array.of(localDates);
        for (int i=0; i<size; ++i) {
            Assert.assertEquals(booleans[i], booleanArray.getBoolean(i), "Booleans match at " + i);
            Assert.assertEquals(integers[i], integerArray.getInt(i), "Integers match at " + i);
            Assert.assertEquals(longs[i], longArray.getLong(i), "Longs match at " + i);
            Assert.assertEquals(doubles[i], doubleArray.getDouble(i), "Doubles match at " + i);
            Assert.assertEquals(localDates[i], objectArray.getValue(i), "LocalDates match at " + i);
        }
    }


    @Test(dataProvider = "types")
    @SuppressWarnings("unchecked")
    public <T> void testFill(Class<T> type, ArrayStyle style) {
        final Array<T> array = Array.of(type, 1000, ArrayType.defaultValue(type), style);
        if (type != Boolean.class) {
            Assert.assertEquals(array.style(), style);
        }
        if (array.typeCode() == ArrayType.BOOLEAN) {
            array.fill((T)Boolean.TRUE);
            for (int i=0; i<array.length(); ++i) {
                Assert.assertEquals(array.getBoolean(i), true, "Values match at " + i);
            }
        } else if (array.typeCode() == ArrayType.INTEGER) {
            array.fill((T)new Integer(17));
            for (int i=0; i<array.length(); ++i) {
                Assert.assertEquals(array.getInt(i), 17, "Values match at " + i);
            }
        } else if (array.typeCode() == ArrayType.LONG) {
            array.fill((T)new Long(32));
            for (int i=0; i<array.length(); ++i) {
                Assert.assertEquals(array.getLong(i), 32, "Values match at " + i);
            }
        } else if (array.typeCode() == ArrayType.DOUBLE) {
            array.fill((T)new Double(56));
            for (int i=0; i<array.length(); ++i) {
                Assert.assertEquals(array.getDouble(i), 56d, "Values match at " + i);
            }
        } else if (array.typeCode() == ArrayType.STRING) {
            array.fill((T)"Hello!");
            for (int i=0; i<array.length(); ++i) {
                Assert.assertEquals(array.getValue(i), "Hello!", "Values match at " + i);
            }
        } else if (array.typeCode() == ArrayType.ENUM) {
            T value = (T) Month.JULY;
            array.fill(value);
            for (int i=0; i<array.length(); ++i) {
                Assert.assertEquals(array.getValue(i), value, "Values match at " + i);
            }
        } else if (array.typeCode() == ArrayType.DATE) {
            T value = (T)new Date();
            array.fill(value);
            for (int i=0; i<array.length(); ++i) {
                Assert.assertEquals(array.getValue(i), value, "Values match at " + i);
            }
        } else if (array.typeCode() == ArrayType.LOCAL_DATE) {
            T value = (T)LocalDate.now();
            array.fill(value);
            for (int i=0; i<array.length(); ++i) {
                Assert.assertEquals(array.getValue(i), value, "Values match at " + i);
            }
        } else if (array.typeCode() == ArrayType.LOCAL_TIME) {
            T value = (T)LocalTime.now();
            array.fill(value);
            for (int i=0; i<array.length(); ++i) {
                Assert.assertEquals(array.getValue(i), value, "Values match at " + i);
            }
        } else if (array.typeCode() == ArrayType.LOCAL_DATETIME) {
            T value = (T)LocalDateTime.now();
            array.fill(value);
            for (int i=0; i<array.length(); ++i) {
                LocalDateTime formattedValue = ((LocalDateTime) value).truncatedTo(ChronoUnit.MILLIS);
                Assert.assertEquals(array.getValue(i), formattedValue, "Values match at " + i);
            }
        } else if (array.typeCode() == ArrayType.ZONED_DATETIME) {
            T value = (T)ZonedDateTime.now();
            array.fill(value);
            for (int i=0; i<array.length(); ++i) {
                 ZonedDateTime formattedValue = ((ZonedDateTime) value).truncatedTo(ChronoUnit.MILLIS);
                 Assert.assertEquals(array.getValue(i), formattedValue, "Values match at " + i);
            }
        } else if (array.typeCode() == ArrayType.OBJECT) {
            T value = (T)new Double(7d);
            array.fill(value);
            for (int i=0; i<array.length(); ++i) {
                Assert.assertEquals(array.getValue(i), value, "Values match at " + i);
            }
        } else if (array.typeCode() == ArrayType.CURRENCY) {
            T value = (T) Currency.getInstance("GBP");
            array.fill(value);
            for (int i=0; i<array.length(); ++i) {
                Assert.assertEquals(array.getValue(i), value, "Values match at " + i);
            }
        } else {
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }


    @Test(dataProvider = "types")
    public <T> void testCopy(Class<T> type, ArrayStyle style) {
        final Array<T> array = createRandomArray(type, 1000, style);
        Assert.assertEquals(array.length(), 1000, "Array length");
        Assert.assertEquals(array.typeCode(), ArrayType.of(type), "The type codes match");
        Array<T> copy1 = array.copy();
        Assert.assertEquals(array, copy1, "The copy equals original");
        int[] indexes = new int[] { 5, 12, 44, 432, 564, 22, 663, 283, 784};
        Array<T> copy2 = array.copy(indexes);
        Assert.assertEquals(copy2.length(), indexes.length, "The copy has expected length");
        for (int i=0; i<indexes.length; ++i) {
            final T actual = copy2.getValue(i);
            final T expected = array.getValue(indexes[i]);
            Assert.assertEquals(actual, expected, "Values match for index " + i);
        }
        Array<T> copy3 = array.copy(40, 234);
        Assert.assertEquals(copy3.length(), 234-40, "The has expected length");
        for (int i=40; i<234; ++i) {
            final T actual = copy3.getValue(i-40);
            final T expected = array.getValue(i);
            Assert.assertEquals(actual, expected, "Values match for index " + i);
        }
    }


    @Test(dataProvider = "types")
    @SuppressWarnings("unchecked")
    public <T> void testFilter(Class<T> type, ArrayStyle style) {
        final Array<T> array = createRandomArray(type, 1000, style);
        if (array.typeCode() == ArrayType.BOOLEAN) {
            final Array<T> filter = array.filter(v -> v.getBoolean());
            Assert.assertTrue(filter.length() < array.length(), "Filter is smaller than source");
            for (int i=0; i<filter.length(); ++i) {
                Assert.assertTrue(filter.getBoolean(i), "Value is true at " + i);
            }
        } else {
            final int[] indexes = IntStream.of(2, 45, 234, 456, 567, 598, 623, 734, 845, 867, 921, 956, 999).toArray();
            final Set<T> includes = array.copy(indexes).stream().values().collect(Collectors.toSet());
            final Array<T> filter = array.filter(v -> includes.contains(v.getValue()));
            Assert.assertEquals(filter.type(), type, "The filter has same type as source");
            filter.forEachValue(v -> Assert.assertTrue(includes.contains(v.getValue()), "Filter contains value: " + v.getValue()));
        }
    }



    @Test()
    public void testLowerAndHigherValueOnSortedStringArray() {
        final Array<String> array = Array.of("a", "c", "e", "g", "i", "k", "m", "o", "q", "s", "u", "w", "y");

        Assert.assertEquals(array.previous("e").map(ArrayValue::getValue).get(), "c", "Lower value than e");
        Assert.assertEquals(array.previous("g").map(ArrayValue::getValue).get(), "e", "Lower value than g");
        Assert.assertEquals(array.previous("i").map(ArrayValue::getValue).get(), "g", "Lower value than i");
        Assert.assertEquals(array.previous("q").map(ArrayValue::getValue).get(), "o", "Lower value than q");
        Assert.assertEquals(array.previous("b").map(ArrayValue::getValue).get(), "a", "Lower value than e");
        Assert.assertEquals(array.previous("f").map(ArrayValue::getValue).get(), "e", "Lower value than g");
        Assert.assertEquals(array.previous("h").map(ArrayValue::getValue).get(), "g", "Lower value than i");
        Assert.assertEquals(array.previous("p").map(ArrayValue::getValue).get(), "o", "Lower value than q");

        Assert.assertEquals(array.next("e").map(ArrayValue::getValue).get(), "g", "Lower value than e");
        Assert.assertEquals(array.next("g").map(ArrayValue::getValue).get(), "i", "Lower value than g");
        Assert.assertEquals(array.next("i").map(ArrayValue::getValue).get(), "k", "Lower value than i");
        Assert.assertEquals(array.next("q").map(ArrayValue::getValue).get(), "s", "Lower value than q");
        Assert.assertEquals(array.next("b").map(ArrayValue::getValue).get(), "c", "Lower value than e");
        Assert.assertEquals(array.next("f").map(ArrayValue::getValue).get(), "g", "Lower value than g");
        Assert.assertEquals(array.next("h").map(ArrayValue::getValue).get(), "i", "Lower value than i");
        Assert.assertEquals(array.next("p").map(ArrayValue::getValue).get(), "q", "Lower value than q");
    }


    @SuppressWarnings("unchecked")
    @Test(dataProvider = "types")
    public <T> void testFirstAndLast(Class<T> type, ArrayStyle style) {
        final Random random = new Random();
        final ArrayType arrayType = ArrayType.of(type);
        if (arrayType == ArrayType.BOOLEAN) {
            final boolean[] values = new boolean[1000];
            for (int i=0; i<values.length; ++i) values[i] = random.nextBoolean();
            final Array<Boolean> array = Array.of((Class<Boolean>)type, values.length, false, style).applyBooleans(v -> values[v.index()]);
            assertFirstAndLast(array, values, arrayType);
        } else if (arrayType == ArrayType.INTEGER) {
            final int[] values = new int[1000];
            for (int i=0; i<values.length; ++i) values[i] = random.nextInt();
            final Array<Integer> array = Array.of((Class<Integer>)type, values.length, 0, style).applyInts(v -> values[v.index()]);
            assertFirstAndLast(array, values, arrayType);
        } else if (arrayType == ArrayType.LONG) {
            final long[] values = new long[1000];
            for (int i=0; i<values.length; ++i) values[i] = random.nextLong();
            final Array<Long> array = Array.of((Class<Long>)type, values.length, 0L, style).applyLongs(v -> values[v.index()]);
            assertFirstAndLast(array, values, arrayType);
        } else if (arrayType == ArrayType.DOUBLE) {
            final double[] values = new double[1000];
            for (int i=0; i<values.length; ++i) values[i] = random.nextDouble();
            final Array<Double> array = Array.of((Class<Double>)type, values.length, 0d, style).applyDoubles(v -> values[v.index()]);
            assertFirstAndLast(array, values, arrayType);
        } else if (arrayType == ArrayType.STRING) {
            final String[] values = new String[1000];
            for (int i=0; i<values.length; ++i) values[i] = "x=" + random.nextDouble();
            final Array<String> array = Array.of((Class<String>)type, values.length, null, style).applyValues(v -> values[v.index()]);
            assertFirstAndLast(array, values, arrayType);
        } else if (arrayType == ArrayType.ENUM) {
            final Month[] values = new Month[1000];
            final Month[] months = Month.values();
            for (int i=0; i<values.length; ++i) values[i] = months[random.nextInt(11)];
            final Array<Month> array = Array.of((Class<Month>)type, values.length, null, style).applyValues(v -> values[v.index()]);
            assertFirstAndLast(array, values, arrayType);
        } else if (arrayType == ArrayType.DATE) {
            final Date[] values = new Date[1000];
            final long start = System.currentTimeMillis();
            for (int i=0; i<values.length; ++i) values[i] = new Date(start + 20000 * i);
            final Array<Date> array = Array.of((Class<Date>)type, values.length, null, style).applyValues(v -> values[v.index()]);
            assertFirstAndLast(array, values, arrayType);
        } else if (arrayType == ArrayType.LOCAL_DATE) {
            final LocalDate[] values = new LocalDate[1000];
            for (int i=0; i<values.length; ++i) values[i] = LocalDate.now().plusDays(i);
            final Array<LocalDate> array = Array.of((Class<LocalDate>)type, values.length, null, style).applyValues(v -> values[v.index()]);
            assertFirstAndLast(array, values, arrayType);
        } else if (arrayType == ArrayType.LOCAL_TIME) {
            final LocalTime[] values = new LocalTime[1000];
            for (int i=0; i<values.length; ++i) values[i] = LocalTime.now().plusSeconds(i);
            final Array<LocalTime> array = Array.of((Class<LocalTime>)type, values.length, null, style).applyValues(v -> values[v.index()]);
            assertFirstAndLast(array, values, arrayType);
        } else if (arrayType == ArrayType.LOCAL_DATETIME) {
            final LocalDateTime[] values = new LocalDateTime[1000];
            for (int i=0; i<values.length; ++i) values[i] = LocalDateTime.now().plusSeconds(i).truncatedTo(ChronoUnit.MILLIS);
            final Array<LocalDateTime> array = Array.of((Class<LocalDateTime>)type, values.length, null, style).applyValues(v -> values[v.index()]);
            assertFirstAndLast(array, values, arrayType);
        } else if (arrayType == ArrayType.ZONED_DATETIME) {
            final ZonedDateTime[] values = new ZonedDateTime[1000];
            for (int i=0; i<values.length; ++i) values[i] = ZonedDateTime.now().plusSeconds(i).truncatedTo(ChronoUnit.MILLIS);
            final Array<ZonedDateTime> array = Array.of((Class<ZonedDateTime>)type, values.length, null, style).applyValues(v -> values[v.index()]);
            assertFirstAndLast(array, values, arrayType);
        } else if (arrayType == ArrayType.OBJECT) {
            if (!style.isMapped()) {
                final Object[] values = new Object[1000];
                for (int i=0; i<values.length; ++i) values[i] = random.nextDouble();
                final Array<Object> array = Array.of((Class<Object>)type, values.length, null, style).applyValues(v -> values[v.index()]);
                assertFirstAndLast(array, values, arrayType);
            }
        } else if (arrayType == ArrayType.CURRENCY) {
            final Currency[] values = new Currency[1000];
            for (int i=0; i<values.length; ++i) values[i] = currencies[random.nextInt(currencies.length)];
            final Array<Currency> array = Array.of((Class<Currency>)type, values.length, null, style).applyValues(v -> values[v.index()]);
            assertFirstAndLast(array, values, arrayType);
        } else {
            throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    /**
     * Asserts first and last elemnts for array
     * @param actual            the actual Morpheus array
     * @param expected          the expected native array
     * @param expectedType      the expected type
     */
    private void assertFirstAndLast(Array<?> actual, Object expected, ArrayType expectedType) {
        Assert.assertEquals(actual.length(), java.lang.reflect.Array.getLength(expected), "Array length");
        Assert.assertEquals(actual.typeCode(), expectedType, "The type codes match");
        Assert.assertTrue(actual.first(v -> true).isPresent(), "First value is present");
        Assert.assertTrue(actual.last(v -> true).isPresent(), "Last value is present");
        Assert.assertEquals(actual.first(v -> true).map(ArrayValue::getValue).get(), java.lang.reflect.Array.get(expected, 0), "First value matches");
        Assert.assertEquals(actual.last(v -> true).map(ArrayValue::getValue).get(), java.lang.reflect.Array.get(expected, actual.length()-1), "Last value is present");
    }


    @Test(dataProvider = "types")
    public <T> void testSwap(Class<T> type, ArrayStyle style) {
        final Array<T> array = createRandomArray(type, 1000, style);
        final Random random = new Random();
        for (int i=0; i<5000; ++i) {
            final int index1 = random.nextInt(array.length());
            final int index2 = random.nextInt(array.length());
            final T value1 = array.getValue(index1);
            final T value2 = array.getValue(index2);
            array.swap(index1, index2);
            Assert.assertEquals(array.getValue(index1), value2, "Value2 is now at index1");
            Assert.assertEquals(array.getValue(index2), value1, "Value1 is now at index2");
        }
    }


    @Test(dataProvider = "types")
    public <T> void testStream(Class<T> type, ArrayStyle style) {
        final int[] index = new int[1];
        final Array<T> array = createRandomArray(type, 10000, style);
        Assert.assertEquals(array.length(), 10000, "Array length matches");
        array.stream().values().forEach(v -> Assert.assertEquals(v, array.getValue(index[0]++), "Values match at index: " + (index[0]-1)));
        Assert.assertEquals(index[0], 10000, "Processed expected number of records");
        index[0] = 0;
        array.stream(500, 600).values().forEach(v -> Assert.assertEquals(v, array.getValue(500 + index[0]++), "Values match at index: " + (500+index[0]-1)));
        Assert.assertEquals(index[0], 100, "Processed expected number of records");
    }


    @Test(dataProvider = "types")
    public <T> void testEquals(Class<T> type, ArrayStyle style) {
        final Array<T> array = createRandomArray(type, 10000, style);
        final Array<T> copy = array.copy();
        Assert.assertTrue(array != copy, "The copy is distinct array instance");
        Assert.assertTrue(array.equals(copy), "The arrays are equal");
        switch (array.typeCode()) {
            case BOOLEAN:   array.setBoolean(500, !array.getBoolean(500));    break;
            case INTEGER:   array.setInt(500, 10);          break;
            case LONG:      array.setLong(500, 20L);        break;
            case DOUBLE:    array.setDouble(500, 34d);      break;
            default:        array.setValue(500, null);      break;
        }
        Assert.assertFalse(array.equals(copy), "The arrays are no longer equal");
        Assert.assertFalse(array.equals(null), "Array is not equal to null");
        Assert.assertFalse(array.equals("xxx"), "Array is not equal to string");

    }


    @Test(expectedExceptions = {ArrayIndexOutOfBoundsException.class})
    public void testEmpty() {
        final Array<String> empty = Array.empty(String.class);
        Assert.assertTrue(empty.length() == 0, "Length is zero");
        Assert.assertEquals(empty.typeCode(), ArrayType.of(String.class), "Type is any");
        Assert.assertTrue(!empty.first(v -> true).isPresent(), "No first element");
        Assert.assertTrue(!empty.last(v -> true).isPresent(), "No last element");
        empty.getValue(100);
    }


    @Test(dataProvider = "classes")
    public <T> void testSingleton(Class<T> type) {
        switch (ArrayType.of(type)) {
            case BOOLEAN:
                Assert.assertEquals(Array.singleton(true).length(), 1, "Singleton length");
                Assert.assertEquals(Array.singleton(true).typeCode(), ArrayType.BOOLEAN, "Singleton type");
                Assert.assertEquals(Array.singleton(true).getBoolean(0), true, "Singleton value");
                break;
            case INTEGER:
                Assert.assertEquals(Array.singleton(5).length(), 1, "Singleton length");
                Assert.assertEquals(Array.singleton(5).typeCode(), ArrayType.INTEGER, "Singleton type");
                Assert.assertEquals(Array.singleton(5).getInt(0), 5, "Singleton value");
                break;
            case LONG:
                Assert.assertEquals(Array.singleton(5L).length(), 1, "Singleton length");
                Assert.assertEquals(Array.singleton(5L).typeCode(), ArrayType.LONG, "Singleton type");
                Assert.assertEquals(Array.singleton(5L).getLong(0), 5L, "Singleton value");
                break;
            case DOUBLE:
                Assert.assertEquals(Array.singleton(5d).length(), 1, "Singleton length");
                Assert.assertEquals(Array.singleton(5d).typeCode(), ArrayType.DOUBLE, "Singleton type");
                Assert.assertEquals(Array.singleton(5d).getDouble(0), 5d, "Singleton value");
                break;
            case LOCAL_DATE:
                Assert.assertEquals(Array.singleton(LocalDate.of(1980,1,1)).length(), 1, "Singleton length");
                Assert.assertEquals(Array.singleton(LocalDate.of(1980,1,1)).typeCode(), ArrayType.LOCAL_DATE, "Singleton type");
                Assert.assertEquals(Array.singleton(LocalDate.of(1980,1,1)).getValue(0), LocalDate.of(1980,1,1), "Singleton value");
                break;
            case LOCAL_TIME:
                Assert.assertEquals(Array.singleton(LocalTime.of(20, 30)).length(), 1, "Singleton length");
                Assert.assertEquals(Array.singleton(LocalTime.of(20, 30)).typeCode(), ArrayType.LOCAL_TIME, "Singleton type");
                Assert.assertEquals(Array.singleton(LocalTime.of(20, 30)).getValue(0), LocalTime.of(20, 30), "Singleton value");
                break;
            case LOCAL_DATETIME:
                Assert.assertEquals(Array.singleton(LocalDateTime.of(1980,1,1,0,0)).length(), 1, "Singleton length");
                Assert.assertEquals(Array.singleton(LocalDateTime.of(1980,1,1,0,0)).typeCode(), ArrayType.LOCAL_DATETIME, "Singleton type");
                Assert.assertEquals(Array.singleton(LocalDateTime.of(1980,1,1,0,0)).getValue(0), LocalDateTime.of(1980,1,1,0,0), "Singleton value");
                break;
            case ZONED_DATETIME:
                ZonedDateTime zdt = ZonedDateTime.of(1980,1,1,0,0,0,0,ZoneId.of("GMT"));
                Assert.assertEquals(Array.singleton(zdt).length(), 1, "Singleton length");
                Assert.assertEquals(Array.singleton(zdt).typeCode(), ArrayType.ZONED_DATETIME, "Singleton type");
                Assert.assertEquals(Array.singleton(zdt).getValue(0), zdt, "Singleton value");
                break;
        }
    }


    @Test(dataProvider = "types")
    public <T> void distinct(Class<T> type, ArrayStyle style) {
        final Array<T> array = createRandomArray(type, 10000, style);
        final Array<T> distinct1 = array.distinct();
        final Array<T> distinct2 = array.distinct(20);
        final Set<T> expected = new HashSet<>(array.toList());
        Assert.assertEquals(distinct1.length(), expected.size());
        Assert.assertTrue(distinct2.length() <= 20);
        distinct1.forEachValue(v -> {
            Assert.assertTrue(expected.contains(v.getValue()));
        });
    }



    @Test(dataProvider = "types", expectedExceptions = { ArrayException.class })
    public <T> void testReadIncompatibleTypeExceptions(Class<T> type, ArrayStyle style) {
        final Array<T> array = createRandomArray(type, 1000, style);
        switch (array.typeCode()) {
            case BOOLEAN:           array.getDouble(0);     break;
            case INTEGER:           array.getBoolean(0);    break;
            case LONG:              array.getBoolean(0);    break;
            case DOUBLE:            array.getLong(0);       break;
            case STRING:            array.getInt(0);        break;
            case ENUM:              array.getBoolean(0);    break;
            case CURRENCY:          array.getLong(0);       break;
            case DATE:              array.getBoolean(0);    break;
            case LOCAL_DATE:        array.getBoolean(0);    break;
            case LOCAL_TIME:        array.getInt(0);        break;
            case LOCAL_DATETIME:    array.getInt(0);        break;
            case ZONED_DATETIME:    array.getInt(0);        break;
            case OBJECT:   throw new ArrayException("All types are supported by OBJECT");
        }
    }


    @Test(dataProvider = "types", expectedExceptions = { ArrayException.class })
    public <T> void testReadOnly1(Class<T> type, ArrayStyle style) {
        final Array<T> array = createRandomArray(type, 1000, style);
        final Array<T> readOnly = array.readOnly();
        Assert.assertEquals(array, readOnly, "The arrays are equal");
        Assert.assertEquals(readOnly, array, "The arrays are equal");
        Assert.assertTrue(readOnly.isReadOnly());
        Assert.assertTrue(!array.isReadOnly());
        switch (array.typeCode()) {
            case BOOLEAN:           readOnly.setBoolean(0, false); break;
            case INTEGER:           readOnly.setInt(0, 1);         break;
            case LONG:              readOnly.setLong(0, 0);        break;
            case DOUBLE:            readOnly.setDouble(0, 0);      break;
            default:                readOnly.setValue(0, null);    break;
        }
    }


    @Test(dataProvider = "types", expectedExceptions = { ArrayException.class })
    public <T> void testReadOnly2(Class<T> type, ArrayStyle style) {
        final Array<T> array = createRandomArray(type, 1000, style);
        final Array<T> readOnly = array.readOnly();
        Assert.assertEquals(array, readOnly, "The arrays are equal");
        Assert.assertEquals(readOnly, array, "The arrays are equal");
        Assert.assertTrue(readOnly.isReadOnly());
        Assert.assertTrue(!array.isReadOnly());
        switch (array.typeCode()) {
            case BOOLEAN:           readOnly.forEachValue(v -> v.setBoolean(false));    break;
            case INTEGER:           readOnly.forEachValue(v -> v.setInt(1));            break;
            case LONG:              readOnly.forEachValue(v -> v.setLong(1L));          break;
            case DOUBLE:            readOnly.forEachValue(v -> v.setDouble(0d));        break;
            default:                readOnly.forEachValue(v -> v.setValue(null));       break;
        }
    }


    @Test(dataProvider = "types")
    public <T> void testArrayOfAny(Class<T> type, ArrayStyle style) {
        final Array<T> array1 = createRandomArray(type, 1000, style);
        final Array<T> array2 = Array.ofObjects(1000, style.isSparse() ? 0.5F : 1F);
        switch (ArrayType.of(type)) {
            case BOOLEAN:   array2.applyBooleans(v -> array1.getBoolean(v.index()));    break;
            case INTEGER:   array2.applyInts(v -> array1.getInt(v.index()));        break;
            case LONG:      array2.applyLongs(v -> array1.getLong(v.index()));      break;
            case DOUBLE:    array2.applyDoubles(v -> array1.getDouble(v.index()));  break;
            default:        array2.applyValues(v -> array1.getValue(v.index()));    break;
        }
        switch (ArrayType.of(type)) {
            case BOOLEAN:   array1.forEachValue(v -> Assert.assertEquals(v.getBoolean(), array2.getBoolean(v.index())));    break;
            case INTEGER:   array1.forEachValue(v -> Assert.assertEquals(v.getInt(), array2.getInt(v.index())));            break;
            case LONG:      array1.forEachValue(v -> Assert.assertEquals(v.getLong(), array2.getLong(v.index())));          break;
            case DOUBLE:    array1.forEachValue(v -> Assert.assertEquals(v.getDouble(), array2.getDouble(v.index())));      break;
            default:        array1.forEachValue(v -> Assert.assertEquals(v.getValue(), array2.getValue(v.index())));        break;
        }
    }


    @Test()
    public void testRandomArrayCreation() {
        Assert.assertTrue(Array.randn(1000).count(v -> v.getDouble() > 0) > 0);
        Assert.assertTrue(Array.randn(1000).count(v -> v.getDouble() < 0) > 0);
    }



    @SuppressWarnings("unchecked")
    public static <T> Array<T> createRandomArray(Class<T> type, int length, ArrayStyle style) {
        final Random random = new Random();
        final float loadFactor = style.isSparse() ? 0.5F : 1F;
        final Array<T> array = style.isMapped() ? Array.map(type, length) : Array.of(type, length, loadFactor);
        switch (ArrayType.of(type)) {
            case OBJECT:            return array.applyDoubles(v -> random.nextDouble());
            case BOOLEAN:           return array.applyBooleans(v -> random.nextBoolean());
            case INTEGER:           return array.applyInts(v -> random.nextInt());
            case LONG:              return array.applyLongs(v -> random.nextLong());
            case DOUBLE:            return array.applyDoubles(v -> random.nextDouble());
            case STRING:            return array.applyValues(v -> (T)("XY=" + random.nextDouble()));
            case CURRENCY:          return array.applyValues(v -> (T)(currencies[random.nextInt(currencies.length)]));
            case ENUM:              return array.applyValues(v -> (T)(Month.values()[random.nextInt(11)]));
            case DATE:              return array.applyValues(v -> (T)(new Date(random.nextInt())));
            case LOCAL_DATE:        return array.applyValues(v -> (T)(LocalDate.now().plusDays(v.index())));
            case LOCAL_TIME:        return array.applyValues(v -> (T)(LocalTime.now().plusSeconds(v.index())));
            case LOCAL_DATETIME:    return array.applyValues(v -> (T)(LocalDateTime.now().plusSeconds(v.index())));
            case ZONED_DATETIME:    return array.applyValues(v -> (T)(ZonedDateTime.now().plusSeconds(v.index())));
            default:    throw new IllegalArgumentException("Unsupported type code " + type);
        }
    }

}
