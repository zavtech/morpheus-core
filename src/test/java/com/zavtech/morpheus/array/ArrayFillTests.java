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
import java.util.Currency;
import java.util.Date;
import java.util.stream.IntStream;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests for the various fill methods on Morpheus arrays
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class ArrayFillTests {

    /**
     * Constructor
     */
    public ArrayFillTests() {
        super();
    }

    @DataProvider(name = "types")
    public Object[][] types() {
        return new ArraysBasicTests().types();
    }


    @DataProvider(name="styles")
    public Object[][] styles() {
        return new Object[][] {
            { ArrayStyle.DENSE },
            { ArrayStyle.SPARSE },
            { ArrayStyle.MAPPED }
        };
    }


    @SuppressWarnings("unchecked")
    @Test(dataProvider = "types")
    public <T> void testFillAll(Class<T> type, ArrayStyle style) {
        final Array<T> array1 = style.isMapped() ? Array.map(type, 1000) : Array.of(type, 1000, style.isSparse() ? 0.5f : 1f);
        switch (array1.typeCode()) {
            case BOOLEAN:
                array1.forEachValue(v -> Assert.assertEquals(v.getBoolean(), false));
                array1.fill((T)Boolean.TRUE);
                array1.forEachValue(v -> Assert.assertEquals(v.getBoolean(), true));
                break;
            case INTEGER:
                array1.forEachValue(v -> Assert.assertEquals(v.getInt(), 0));
                array1.fill((T)new Integer(25));
                array1.forEachValue(v -> Assert.assertEquals(v.getInt(), 25));
                break;
            case LONG:
                array1.forEachValue(v -> Assert.assertEquals(v.getLong(), 0L));
                array1.fill((T)new Long(18));
                array1.forEachValue(v -> Assert.assertEquals(v.getLong(), 18L));
                break;
            case DOUBLE:
                array1.forEachValue(v -> Assert.assertTrue(Double.isNaN(v.getDouble())));
                array1.fill((T)new Double(76.34));
                array1.forEachValue(v -> Assert.assertEquals(v.getDouble(), 76.34d));
                break;
            case STRING:
                array1.forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                array1.fill((T)"Hello There");
                array1.forEachValue(v -> Assert.assertEquals(v.getValue(), "Hello There"));
                break;
            case ENUM:
                array1.forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                array1.fill((T)Month.JANUARY);
                array1.forEachValue(v -> Assert.assertEquals(v.getValue(), Month.JANUARY));
                break;
            case DATE:
                array1.forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                array1.fill((T) new Date(0));
                array1.forEachValue(v -> Assert.assertEquals(v.getValue(), new Date(0)));
                break;
            case LOCAL_DATE:
                array1.forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                array1.fill((T) LocalDate.of(1990, 1, 1));
                array1.forEachValue(v -> Assert.assertEquals(v.getValue(), LocalDate.of(1990, 1, 1)));
                break;
            case LOCAL_TIME:
                array1.forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                array1.fill((T) LocalTime.of(23, 55));
                array1.forEachValue(v -> Assert.assertEquals(v.getValue(), LocalTime.of(23, 55)));
                break;
            case LOCAL_DATETIME:
                array1.forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                array1.fill((T) LocalDateTime.of(2000, 11, 12, 14, 5));
                array1.forEachValue(v -> Assert.assertEquals(v.getValue(), LocalDateTime.of(2000, 11, 12, 14, 5)));
                break;
            case ZONED_DATETIME:
                array1.forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                array1.fill((T) ZonedDateTime.of(2000, 11, 12, 14, 5, 0, 0, ZoneId.of("GMT")));
                array1.forEachValue(v -> Assert.assertEquals(v.getValue(), ZonedDateTime.of(2000, 11, 12, 14, 5, 0, 0, ZoneId.of("GMT"))));
                break;
            case CURRENCY:
                array1.forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                array1.fill((T) Currency.getInstance("GBP"));
                array1.forEachValue(v -> Assert.assertEquals(v.getValue(), Currency.getInstance("GBP")));
                break;
            case OBJECT:
                array1.forEachValue(v -> Assert.assertEquals(v.getValue(), array1.defaultValue()));
                array1.fill((T)new Double(76.34));
                array1.forEachValue(v -> Assert.assertEquals(v.getDouble(), 76.34d));
                break;
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }


    @SuppressWarnings("unchecked")
    @Test(dataProvider = "types")
    public <T> void testFillSubset(Class<T> type, ArrayStyle style) {
        final Array<T> array1 = style.isMapped() ? Array.map(type, 1000) : Array.of(type, 1000, style.isSparse() ? 0.5f : 1f);
        switch (array1.typeCode()) {
            case BOOLEAN:
                array1.forEachValue(v -> Assert.assertEquals(v.getBoolean(), false));
                array1.fill((T) Boolean.TRUE, 200, 400);
                array1.copy(0, 200).forEachValue(v -> Assert.assertEquals(v.getBoolean(), false));
                array1.copy(200, 400).forEachValue(v -> Assert.assertEquals(v.getBoolean(), true));
                array1.copy(400, array1.length()).forEachValue(v -> Assert.assertEquals(v.getBoolean(), false));
                break;
            case INTEGER:
                array1.forEachValue(v -> Assert.assertEquals(v.getInt(), 0));
                array1.fill((T)new Integer(25), 200, 400);
                array1.copy(0, 200).forEachValue(v -> Assert.assertEquals(v.getInt(), 0));
                array1.copy(200, 400).forEachValue(v -> Assert.assertEquals(v.getInt(), 25));
                array1.copy(400, array1.length()).forEachValue(v -> Assert.assertEquals(v.getInt(), 0));
                break;
            case LONG:
                array1.forEachValue(v -> Assert.assertEquals(v.getLong(), 0L));
                array1.fill((T)new Long(18), 200, 400);
                array1.copy(0, 200).forEachValue(v -> Assert.assertEquals(v.getLong(), 0L));
                array1.copy(200, 400).forEachValue(v -> Assert.assertEquals(v.getLong(), 18L));
                array1.copy(400, array1.length()).forEachValue(v -> Assert.assertEquals(v.getLong(), 0L));
                break;
            case DOUBLE:
                array1.forEachValue(v -> Assert.assertEquals(v.getDouble(), Double.NaN));
                array1.fill((T)new Double(76.34), 200, 400);
                array1.copy(0, 200).forEachValue(v -> Assert.assertEquals(v.getDouble(), Double.NaN));
                array1.copy(200, 400).forEachValue(v -> Assert.assertEquals(v.getDouble(), 76.34d));
                array1.copy(400, array1.length()).forEachValue(v -> Assert.assertEquals(v.getDouble(), Double.NaN));
                break;
            case STRING:
                array1.forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                array1.fill((T)"Hello There", 200, 400);
                array1.copy(0, 200).forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                array1.copy(200, 400).forEachValue(v -> Assert.assertEquals(v.getValue(), "Hello There"));
                array1.copy(400, array1.length()).forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                break;
            case ENUM:
                array1.forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                array1.fill((T)Month.JUNE, 200, 400);
                array1.copy(0, 200).forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                array1.copy(200, 400).forEachValue(v -> Assert.assertEquals(v.getValue(), Month.JUNE));
                array1.copy(400, array1.length()).forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                break;
            case DATE:
                array1.forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                array1.fill((T) new Date(0), 200, 400);
                array1.copy(0, 200).forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                array1.copy(200, 400).forEachValue(v -> Assert.assertEquals(v.getValue(), new Date(0)));
                array1.copy(400, array1.length()).forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                break;
            case LOCAL_DATE:
                array1.forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                array1.fill((T) LocalDate.of(1990, 1, 1), 200, 400);
                array1.copy(0, 200).forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                array1.copy(200, 400).forEachValue(v -> Assert.assertEquals(v.getValue(), LocalDate.of(1990, 1, 1)));
                array1.copy(400, array1.length()).forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                break;
            case LOCAL_TIME:
                array1.forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                array1.fill((T) LocalTime.of(23, 55), 200, 400);
                array1.copy(0, 200).forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                array1.copy(200, 400).forEachValue(v -> Assert.assertEquals(v.getValue(), LocalTime.of(23, 55)));
                array1.copy(400, array1.length()).forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                break;
            case LOCAL_DATETIME:
                array1.forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                array1.fill((T) LocalDateTime.of(2000, 11, 12, 14, 5), 200, 400);
                array1.copy(0, 200).forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                array1.copy(200, 400).forEachValue(v -> Assert.assertEquals(v.getValue(), LocalDateTime.of(2000, 11, 12, 14, 5)));
                array1.copy(400, array1.length()).forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                break;
            case ZONED_DATETIME:
                array1.forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                array1.fill((T) ZonedDateTime.of(2000, 11, 12, 14, 5, 0, 0, ZoneId.of("GMT")), 200, 400);
                array1.copy(0, 200).forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                array1.copy(200, 400).forEachValue(v -> Assert.assertEquals(v.getValue(), ZonedDateTime.of(2000, 11, 12, 14, 5, 0, 0, ZoneId.of("GMT"))));
                array1.copy(400, array1.length()).forEachValue(v -> Assert.assertEquals(v.getValue(), null));
                break;
            case CURRENCY:
                array1.forEachValue(v -> Assert.assertEquals(v.getValue(), array1.defaultValue()));
                array1.fill((T)Currency.getInstance("GBP"), 200, 400);
                array1.copy(0, 200).forEachValue(v -> Assert.assertEquals(v.getValue(), array1.defaultValue()));
                array1.copy(200, 400).forEachValue(v -> Assert.assertEquals(v.getValue(), Currency.getInstance("GBP")));
                array1.copy(400, array1.length()).forEachValue(v -> Assert.assertEquals(v.getValue(), array1.defaultValue()));
                break;
            case OBJECT:
                array1.forEachValue(v -> Assert.assertEquals(v.getValue(), array1.defaultValue()));
                array1.fill((T)new Double(76.34), 200, 400);
                array1.copy(0, 200).forEachValue(v -> Assert.assertEquals(v.getValue(), array1.defaultValue()));
                array1.copy(200, 400).forEachValue(v -> Assert.assertEquals(v.getDouble(), 76.34d));
                array1.copy(400, array1.length()).forEachValue(v -> Assert.assertEquals(v.getValue(), array1.defaultValue()));
                break;
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }


    @Test(dataProvider = "styles")
    public void testWithDefaultValue(ArrayStyle style) {
        final LocalDate defaultValue = LocalDate.of(2000, 1, 1);
        final Array<LocalDate> array = style.isMapped() ? Array.map(LocalDate.class, 1000, defaultValue) : Array.of(LocalDate.class, 1000, defaultValue, style.isSparse() ? 0.5f : 1f);
        array.forEachValue(v -> Assert.assertEquals(v.getValue(), defaultValue));
        array.fill(null, 20, 30);
        IntStream.range(0, 20).forEach(i -> Assert.assertEquals(array.getValue(i), defaultValue));
        IntStream.range(20, 30).forEach(i -> Assert.assertEquals(array.getValue(i), null));
        IntStream.range(30, array.length()).forEach(i -> Assert.assertEquals(array.getValue(i), defaultValue));
    }

}
