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
package com.zavtech.morpheus.array.dense;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Currency;
import java.util.Date;
import java.util.TimeZone;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayType;
import com.zavtech.morpheus.array.ArrayFactory;
import com.zavtech.morpheus.array.coding.IntCoding;
import com.zavtech.morpheus.array.coding.LongCoding;

/**
 * An ArrayFactory.Constructor implementation designed to manufacture dense Morpheus Arrays.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class DenseArrayConstructor implements ArrayFactory.Constructor {

    private static final IntCoding<Year> yearCoding = new IntCoding.OfYear();
    private static final IntCoding<Currency> currencyCoding = new IntCoding.OfCurrency();
    private static final IntCoding<ZoneId> zoneIdCoding = IntCoding.ofZoneId();
    private static final IntCoding<TimeZone> timeZoneCoding = IntCoding.ofTimeZone();
    private static final LongCoding<Date> dateCoding = LongCoding.ofDate();
    private static final LongCoding<Instant> instantCoding = LongCoding.ofInstant();
    private static final LongCoding<LocalDate> localDateCoding = LongCoding.ofLocalDate();
    private static final LongCoding<LocalTime> localTimeCoding = LongCoding.ofLocalTime();
    private static final LongCoding<LocalDateTime> localDateTimeCoding = LongCoding.ofLocalDateTime();


    /**
     * Constructor
     */
    public DenseArrayConstructor() {
        super();
    }


    @Override()
    public final <T> Array<T> apply(Class<T> type, int length, T defaultValue) {
        return apply(type, length, defaultValue, null);
    }


    @Override()
    @SuppressWarnings("unchecked")
    public final <T> Array<T> apply(Class<T> type, int length, T defaultValue, String path) {
        if (type.isEnum()) {
            final IntCoding<T> enumCoding = (IntCoding<T>) IntCoding.ofEnum((Class<Enum>) type);
            return new DenseArrayWithIntCoding<>(length, defaultValue, enumCoding);
        } else {
            switch (ArrayType.of(type)) {
                case OBJECT:            return new DenseArrayOfObjects<>(type, length, defaultValue);
                case STRING:            return new DenseArrayOfObjects<>(type, length, defaultValue);
                case BOOLEAN:           return (Array<T>)new DenseArrayOfBooleans(length, (Boolean)defaultValue);
                case INTEGER:           return (Array<T>)new DenseArrayOfInts(length, (Integer)defaultValue);
                case LONG:              return (Array<T>)new DenseArrayOfLongs(length, (Long)defaultValue);
                case DOUBLE:            return (Array<T>)new DenseArrayOfDoubles(length, (Double)defaultValue);
                case CURRENCY:          return (Array<T>)new DenseArrayWithIntCoding<>(length, (Currency)defaultValue, currencyCoding);
                case YEAR:              return (Array<T>)new DenseArrayWithIntCoding<>(length, (Year)defaultValue, yearCoding);
                case ZONE_ID:           return (Array<T>)new DenseArrayWithIntCoding<>(length, (ZoneId)defaultValue, zoneIdCoding);
                case TIME_ZONE:         return (Array<T>)new DenseArrayWithIntCoding<>(length, (TimeZone)defaultValue, timeZoneCoding);
                case DATE:              return (Array<T>)new DenseArrayWithLongCoding<>(length, (Date)defaultValue, dateCoding);
                case INSTANT:           return (Array<T>)new DenseArrayWithLongCoding<>(length, (Instant)defaultValue, instantCoding);
                case LOCAL_DATE:        return (Array<T>)new DenseArrayWithLongCoding<>(length, (LocalDate)defaultValue, localDateCoding);
                case LOCAL_TIME:        return (Array<T>)new DenseArrayWithLongCoding<>(length, (LocalTime)defaultValue, localTimeCoding);
                case LOCAL_DATETIME:    return (Array<T>)new DenseArrayWithLongCoding<>(length, (LocalDateTime)defaultValue, localDateTimeCoding);
                case ZONED_DATETIME:    return (Array<T>)new DenseArrayOfZonedDateTimes(length, (ZonedDateTime) defaultValue);
                default:                return new DenseArrayOfObjects<>(type, length, defaultValue);
            }
        }
    }
}
