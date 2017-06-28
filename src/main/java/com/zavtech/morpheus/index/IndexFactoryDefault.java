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
package com.zavtech.morpheus.index;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Iterator;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayType;
import com.zavtech.morpheus.array.coding.IntCoding;
import com.zavtech.morpheus.array.coding.LongCoding;

/**
 * The default IndexFactory implementation
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
class IndexFactoryDefault extends IndexFactory {

    private static final IntCoding<Year> yearCoding = IntCoding.ofYear();
    private static final LongCoding<Date> dateCoding = LongCoding.ofDate();
    private static final LongCoding<Instant> instantCoding = LongCoding.ofInstant();
    private static final LongCoding<LocalDate> localDateCoding = LongCoding.ofLocalDate();
    private static final LongCoding<LocalTime> localTimeCoding = LongCoding.ofLocalTime();
    private static final LongCoding<LocalDateTime> localDateTimeCoding = LongCoding.ofLocalDateTime();
    private static final LongCoding<ZonedDateTime> zonedDateTimeCoding = LongCoding.ofZonedDateTime();


    @Override
    @SuppressWarnings("unchecked")
    public <K> Index<K> create(Iterable<K> keys) {
        final ArrayType type = typeOf(keys);
        switch (type) {
            case INTEGER:           return (Index<K>)new IndexOfInts((Iterable<Integer>)keys);
            case LONG:              return (Index<K>)new IndexOfLongs((Iterable<Long>)keys);
            case DOUBLE:            return (Index<K>)new IndexOfDoubles((Iterable<Double>)keys);
            case STRING:            return (Index<K>)new IndexOfStrings((Iterable<String>)keys);
            case YEAR:              return (Index<K>)new IndexWithIntCoding<>((Iterable<Year>)keys, yearCoding);
            case DATE:              return (Index<K>)new IndexWithLongCoding<>((Iterable<Date>)keys, dateCoding);
            case INSTANT:           return (Index<K>)new IndexWithLongCoding<>((Iterable<Instant>)keys, instantCoding);
            case LOCAL_DATE:        return (Index<K>)new IndexWithLongCoding<>((Iterable<LocalDate>)keys, localDateCoding);
            case LOCAL_TIME:        return (Index<K>)new IndexWithLongCoding<>((Iterable<LocalTime>)keys, localTimeCoding);
            case LOCAL_DATETIME:    return (Index<K>)new IndexWithLongCoding<>((Iterable<LocalDateTime>)keys, localDateTimeCoding);
            case ZONED_DATETIME:    return (Index<K>)new IndexWithLongCoding<>((Iterable<ZonedDateTime>)keys, zonedDateTimeCoding);
            default:                return new IndexOfObjects<>(keys);
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public <K> Index<K> create(Class<K> keyType, int initialSize) {
        switch (ArrayType.of(keyType)) {
            case INTEGER:           return (Index<K>)new IndexOfInts(initialSize);
            case LONG:              return (Index<K>)new IndexOfLongs(initialSize);
            case DOUBLE:            return (Index<K>)new IndexOfDoubles(initialSize);
            case STRING:            return (Index<K>)new IndexOfStrings(initialSize);
            case YEAR:              return (Index<K>)new IndexWithIntCoding<>((Class<Year>)keyType, yearCoding, initialSize);
            case DATE:              return (Index<K>)new IndexWithLongCoding<>((Class<Date>)keyType, dateCoding, initialSize);
            case INSTANT:           return (Index<K>)new IndexWithLongCoding<>((Class<Instant>)keyType, instantCoding, initialSize);
            case LOCAL_DATE:        return (Index<K>)new IndexWithLongCoding<>((Class<LocalDate>)keyType, localDateCoding, initialSize);
            case LOCAL_TIME:        return (Index<K>)new IndexWithLongCoding<>((Class<LocalTime>)keyType, localTimeCoding, initialSize);
            case LOCAL_DATETIME:    return (Index<K>)new IndexWithLongCoding<>((Class<LocalDateTime>)keyType, localDateTimeCoding, initialSize);
            case ZONED_DATETIME:    return (Index<K>)new IndexWithLongCoding<>((Class<ZonedDateTime>)keyType, zonedDateTimeCoding, initialSize);
            default:                return new IndexOfObjects<>(keyType, initialSize);
        }
    }

    /**
     * Returns the array type from the Iterable
     * @param keys  the Iterable keys
     * @param <K>   the key type
     * @return      the corresponding array type
     */
    private <K> ArrayType typeOf(Iterable<K> keys) {
        if (keys instanceof Array) {
            final Array<?> array = (Array<?>)keys;
            return ArrayType.of(array.type());
        } else if (keys instanceof Index) {
            final Index<?> index = (Index<?>)keys;
            return ArrayType.of(index.type());
        } else {
            final Iterator<K> iterator = keys.iterator();
            final K firstKey = iterator.hasNext() ? iterator.next() : null;
            if (firstKey == null) {
                return ArrayType.OBJECT;
            } else {
                final Class<?> keyType = firstKey.getClass();
                return ArrayType.of(keyType);
            }
        }
    }
}
