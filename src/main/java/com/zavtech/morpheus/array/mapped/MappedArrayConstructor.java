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
package com.zavtech.morpheus.array.mapped;

import java.io.File;
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
import java.util.UUID;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayException;
import com.zavtech.morpheus.array.ArrayFactory;
import com.zavtech.morpheus.array.ArrayType;
import com.zavtech.morpheus.array.coding.IntCoding;
import com.zavtech.morpheus.array.coding.LongCoding;

/**
 * An ArrayFactory.Constructor implementation designed to manufacture memoey mapped Morpheus Arrays.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class MappedArrayConstructor implements ArrayFactory.Constructor {

    private static File memoryMappedBasePath = new File(System.getProperty("morpheus.array.path", System.getProperty("user.home") + "/.morpheus/temp"));

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
    public MappedArrayConstructor() {
        super();
    }


    @Override()
    public final <T> Array<T> apply(Class<T> type, int length, T defaultValue) {
        return apply(type, length, defaultValue, null);
    }


    @Override
    @SuppressWarnings("unchecked")
    public <T> Array<T> apply(Class<T> type, int length, T defaultValue, String path) {
        final File file = path == null ? randomFile(true) : createDir(new File(path));
        if (type.isEnum()) {
            final IntCoding<T> enumCoding = (IntCoding<T>)IntCoding.ofEnum((Class<Enum>) type);
            return new MappedArrayWithIntCoding<>(length, defaultValue, enumCoding, file);
        } else {
            switch (ArrayType.of(type)) {
                case BOOLEAN:           return (Array<T>)new MappedArrayOfBooleans(length, (Boolean)defaultValue, file);
                case INTEGER:           return (Array<T>)new MappedArrayOfInts(length, (Integer)defaultValue, file);
                case LONG:              return (Array<T>)new MappedArrayOfLongs(length, (Long)defaultValue, file);
                case DOUBLE:            return (Array<T>)new MappedArrayOfDoubles(length, (Double)defaultValue, file);
                case CURRENCY:          return (Array<T>)new MappedArrayWithIntCoding<>(length, (Currency)defaultValue, currencyCoding, file);
                case YEAR:              return (Array<T>)new MappedArrayWithIntCoding<>(length, (Year)defaultValue, yearCoding, file);
                case ZONE_ID:           return (Array<T>)new MappedArrayWithIntCoding<>(length, (ZoneId)defaultValue, zoneIdCoding, file);
                case TIME_ZONE:         return (Array<T>)new MappedArrayWithIntCoding<>(length, (TimeZone)defaultValue, timeZoneCoding, file);
                case DATE:              return (Array<T>)new MappedArrayWithLongCoding<>(length, (Date)defaultValue, dateCoding, file);
                case INSTANT:           return (Array<T>)new MappedArrayWithLongCoding<>(length, (Instant)defaultValue, instantCoding, file);
                case LOCAL_DATE:        return (Array<T>)new MappedArrayWithLongCoding<>(length, (LocalDate)defaultValue, localDateCoding, file);
                case LOCAL_TIME:        return (Array<T>)new MappedArrayWithLongCoding<>(length, (LocalTime)defaultValue, localTimeCoding, file);
                case LOCAL_DATETIME:    return (Array<T>)new MappedArrayWithLongCoding<>(length, (LocalDateTime)defaultValue, localDateTimeCoding, file);
                case ZONED_DATETIME:    return (Array<T>)new MappedArrayOfZonedDateTimes(length, (ZonedDateTime)defaultValue, file);
                default:                throw new UnsupportedOperationException("Data type currently not supported for memory mapped arrays: " + type);
            }
        }
    }

    /**
     * Returns a newly created random file to store an array
     * @return      newly created random file
     */
    static File randomFile(boolean deleteOnExit) {
        final File file = new File(memoryMappedBasePath, UUID.randomUUID().toString() + ".dat");
        if (deleteOnExit) {
            file.deleteOnExit();
        }
        return createDir(file);
    }


    /**
     * Creates the directories for the mapped file if they do not already exist
     * @param file      the memory mapped file handle
     * @return          the same as arg
     */
    private static File createDir(File file) {
        if (!file.exists()) {
            final File dir = file.getParentFile();
            if (dir != null && !dir.exists()) {
                if (!dir.mkdirs()) {
                    throw new ArrayException("Unable to create directory for memory mapped file at: " + file.getAbsolutePath());
                }
            }
        }
        return file;
    }
}
