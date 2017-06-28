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
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Currency;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests for some of the functional methods on the Array class.
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class ArrayFuncTests {

    private Class<?>[] types = new Class<?>[] {
        Boolean.class,
        Integer.class,
        Long.class,
        Double.class,
        String.class,
        Month.class,
        Currency.class,
        LocalDate.class,
        LocalTime.class,
        LocalDateTime.class,
        ZonedDateTime.class
    };


    @DataProvider(name = "arrays")
    public Object[][] arrays() {
        final List<Object[]> argList = new ArrayList<>();
        for (ArrayStyle style : ArrayStyle.values()) {
            for (Class<?> clazz : types) {
                if (style.isMapped()) {
                    final ArrayType type = ArrayType.of(clazz);
                    if (!type.isString() && !type.isObject()) {
                        final Array<?> array = ArraysBasicTests.createRandomArray(clazz, 1000, style);
                        argList.add(new Object[]  { array });
                    }
                } else {
                    final Array<?> array = ArraysBasicTests.createRandomArray(clazz, 1000, style);
                    argList.add(new Object[]  { array });
                }
            }
        }
        return argList.toArray(new Object[argList.size()][]);
    }


    @Test(dataProvider = "arrays")
    public <T> void testApply(Array<T> source) {
        final Array<T> target = Array.of(source.type(), source.length());
        Assert.assertFalse(target.equals(source), "The arrays do not match before apply");
        switch (source.typeCode()) {
            case BOOLEAN:           target.applyBooleans(v -> source.getBoolean(v.index()));    break;
            case INTEGER:           target.applyInts(v -> source.getInt(v.index()));            break;
            case LONG:              target.applyLongs(v -> source.getLong(v.index()));          break;
            case DOUBLE:            target.applyDoubles(v -> source.getDouble(v.index()));      break;
            case STRING:            target.applyValues(v -> source.getValue(v.index()));        break;
            case ENUM:              target.applyValues(v -> source.getValue(v.index()));        break;
            case CURRENCY:          target.applyValues(v -> source.getValue(v.index()));        break;
            case LOCAL_DATE:        target.applyValues(v -> source.getValue(v.index()));        break;
            case LOCAL_TIME:        target.applyValues(v -> source.getValue(v.index()));        break;
            case LOCAL_DATETIME:    target.applyValues(v -> source.getValue(v.index()));        break;
            case ZONED_DATETIME:    target.applyValues(v -> source.getValue(v.index()));        break;
        }
        Assert.assertTrue(target.equals(source), "The arrays match after apply");
    }


    @SuppressWarnings("unchecked")
    @Test(dataProvider = "arrays")
    public <T> void testMap(Array<T> source) {
        Array<T> mapped = null;
        switch (source.typeCode()) {
            case BOOLEAN:           mapped = (Array<T>)source.mapToBooleans(v -> source.getBoolean(v.index()));    break;
            case INTEGER:           mapped = (Array<T>)source.mapToInts(v -> source.getInt(v.index()));            break;
            case LONG:              mapped = (Array<T>)source.mapToLongs(v -> source.getLong(v.index()));          break;
            case DOUBLE:            mapped = (Array<T>)source.mapToDoubles(v -> source.getDouble(v.index()));   break;
            case STRING:            mapped = source.map(v -> source.getValue(v.index()));        break;
            case ENUM:              mapped = source.map(v -> source.getValue(v.index()));        break;
            case CURRENCY:          mapped = source.map(v -> source.getValue(v.index()));        break;
            case LOCAL_DATE:        mapped = source.map(v -> source.getValue(v.index()));        break;
            case LOCAL_TIME:        mapped = source.map(v -> source.getValue(v.index()));        break;
            case LOCAL_DATETIME:    mapped = source.map(v -> source.getValue(v.index()));        break;
            case ZONED_DATETIME:    mapped = source.map(v -> source.getValue(v.index()));        break;
        }
        Assert.assertTrue(mapped != null, "Mapped array was generated");
        //Assert.assertTrue(mapped != source, "The arrays match apply apply");  todo: put this back in
        Assert.assertNotEquals(System.identityHashCode(source), System.identityHashCode(mapped), "The mapped array is not the same instance");
    }


    @Test(dataProvider = "arrays")
    public <T> void testStreams1(Array<T> array) {
        final Array<T> copy = array.stream().values().collect(ArrayUtils.toArray());
        Assert.assertEquals(copy, array, "The copied array matches source");
        if (array.typeCode() == ArrayType.INTEGER) {
            final int[] ints = array.stream().ints().toArray();
            array.forEachValue(v -> Assert.assertEquals(ints[v.index()], v.getInt(), "Values match for index: " + v.index()));
        } else if (array.typeCode() == ArrayType.LONG) {
            final long[] longs = array.stream().longs().toArray();
            array.forEachValue(v -> Assert.assertEquals(longs[v.index()], v.getLong(), "Values match for index: " + v.index()));
        } else if (array.typeCode() == ArrayType.DOUBLE) {
            final double[] doubles = array.stream().doubles().toArray();
            array.forEachValue(v -> Assert.assertEquals(doubles[v.index()], v.getDouble(), "Values match for index: " + v.index()));
        } else {
            final Object[] values = array.stream().values().toArray();
            array.forEachValue(v -> Assert.assertEquals(values[v.index()], v.getValue(), "Values match for index: " + v.index()));
        }
    }


    @Test(dataProvider = "arrays")
    public <T> void testStreams2(Array<T> array) {
        final Array<T> copy = array.stream(10, 20).values().collect(ArrayUtils.toArray());
        Assert.assertEquals(copy, array.copy(10, 20), "The copied array matches source");
        if (array.typeCode() == ArrayType.INTEGER) {
            final int[] ints = array.stream(10, 20).ints().toArray();
            array.copy(10, 20).forEachValue(v -> Assert.assertEquals(ints[v.index()], v.getInt(), "Values match for index: " + v.index()));
        } else if (array.typeCode() == ArrayType.LONG) {
            final long[] longs = array.stream(10, 20).longs().toArray();
            array.copy(10, 20).forEachValue(v -> Assert.assertEquals(longs[v.index()], v.getLong(), "Values match for index: " + v.index()));
        } else if (array.typeCode() == ArrayType.DOUBLE) {
            final double[] doubles = array.stream(10, 20).doubles().toArray();
            array.copy(10, 20).forEachValue(v -> Assert.assertEquals(doubles[v.index()], v.getDouble(), "Values match for index: " + v.index()));
        } else {
            final Object[] values = array.stream(10, 20).values().toArray();
            array.copy(10, 20).forEachValue(v -> Assert.assertEquals(values[v.index()], v.getValue(), "Values match for index: " + v.index()));
        }
    }


    @Test(dataProvider = "arrays")
    public <T> void testForEachValue(Array<T> array) {
        final Array<T> array1 = array.stream().values().collect(ArrayUtils.toArray());
        final Array<T> array2 = array1.copy();
        Assert.assertEquals(array1, array, "The array1 matches source");
        Assert.assertEquals(array2, array, "The array2 matches source");
        array2.shuffle(5);
        Assert.assertNotEquals(array2, array, "The shuffled array does not match source");
        switch (array.typeCode()) {
            case BOOLEAN:
                array.forEachValue(v -> Assert.assertEquals(v.getBoolean(), array1.getBoolean(v.index()), "Values match for index: " + v.index()));
                array.forEachValue(v -> v.setBoolean(array2.getBoolean(v.index())));
                array.forEachValue(v -> Assert.assertEquals(v.getBoolean(), array2.getBoolean(v.index()), "Values match for index: " + v.index()));
                break;
            case INTEGER:
                array.forEachValue(v -> Assert.assertEquals(v.getInt(), array1.getInt(v.index()), "Values match for index: " + v.index()));
                array.forEachValue(v -> v.setInt(array2.getInt(v.index())));
                array.forEachValue(v -> Assert.assertEquals(v.getInt(), array2.getInt(v.index()), "Values match for index: " + v.index()));
                break;
            case LONG:
                array.forEachValue(v -> Assert.assertEquals(v.getLong(), array1.getLong(v.index()), "Values match for index: " + v.index()));
                array.forEachValue(v -> v.setLong(array2.getLong(v.index())));
                array.forEachValue(v -> Assert.assertEquals(v.getLong(), array2.getLong(v.index()), "Values match for index: " + v.index()));
                break;
            case DOUBLE:
                array.forEachValue(v -> Assert.assertEquals(v.getDouble(), array1.getDouble(v.index()), "Values match for index: " + v.index()));
                array.forEachValue(v -> v.setDouble(array2.getDouble(v.index())));
                array.forEachValue(v -> Assert.assertEquals(v.getDouble(), array2.getDouble(v.index()), "Values match for index: " + v.index()));
                break;
            default:
                array.forEachValue(v -> Assert.assertEquals(v.getValue(), array1.getValue(v.index()), "Values match for index: " + v.index()));
                array.forEachValue(v -> v.setValue(array2.getValue(v.index())));
                array.forEachValue(v -> Assert.assertEquals(v.getValue(), array2.getValue(v.index()), "Values match for index: " + v.index()));
                break;
        }
    }


}
