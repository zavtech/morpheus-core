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
package com.zavtech.morpheus.array.coding;

import java.time.Year;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Currency;
import java.util.TimeZone;
import java.util.stream.IntStream;

import com.zavtech.morpheus.util.IntComparator;
import com.zavtech.morpheus.util.SortAlgorithm;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

/**
 * An interface that exposes a coding between object values and corresponding int code
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public interface IntCoding<T> extends Coding<T> {

    /**
     * Returns the code for the value specified
     * @param value the value, which can be null
     * @return      the code for value
     */
    int getCode(T value);

    /**
     * Returns the value for the code specified
     * @param code  the code for requested value
     * @return      the value match, which can be null
     */
    T getValue(int code);


    /**
     * Returns a new coding for the Year class
     * @return  the newly created coding
     */
    static IntCoding<Year> ofYear() {
        return new OfYear();
    }

    /**
     * Returns a new coding for the ZoneId class
     * @return  the newly created coding
     */
    static IntCoding<ZoneId> ofZoneId() {
        return new OfZoneId();
    }

    /**
     * Returns a new coding for the TimeZone class
     * @return  the newly created coding
     */
    static IntCoding<TimeZone> ofTimeZone() {
        return new OfTimeZone();
    }

    /**
     * Returns a new coding for the Currency class
     * @return  the newly created coding
     */
    static IntCoding<Currency> ofCurrency() {
        return new OfCurrency();
    }

    /**
     * Returns a new coding for the enum specified
     * @param type  the enum type
     * @param <T>   the type
     * @return      the newly created coding
     */
    static <T extends Enum> IntCoding<T> ofEnum(Class<T> type) {
        return new OfEnum<>(type);
    }


    /**
     * An IntCoding implementation for an Enum class
     */
    class OfEnum<T extends Enum> extends BaseCoding<T> implements IntCoding<T> {

        private static final long serialVersionUID = 1L;

        private final T[] values;
        private final int[] codes;

        /**
         * Constructor
         * @param type  the enum class
         */
        @SuppressWarnings("unchecked")
        OfEnum(Class<T> type) {
            super(type);
            this.values = type.getEnumConstants();
            this.codes = IntStream.range(0, values.length).toArray();
            final IntComparator comparator = (i, j) -> values[i].compareTo(values[j]);
            SortAlgorithm.getDefault(false).sort(0, values.length, comparator, (i, j) -> {
                final T v1 = values[i]; values[i] = values[j]; values[j] = v1;
                final int code = codes[i]; codes[i] = codes[j]; codes[j] = code;
            });
        }

        @Override
        public final int getCode(T value) {
            return value == null ? -1 : codes[value.ordinal()];
        }

        @Override
        public final T getValue(int code) {
            return code < 0 ? null : values[code];
        }
    }



    /**
     * An IntCoding implementation for the Year class
     */
    class OfYear extends BaseCoding<Year> implements IntCoding<Year> {

        private static final long serialVersionUID = 1L;

        /**
         * Constructor
         */
        public OfYear() {
            super(Year.class);
        }

        @Override
        public final int getCode(Year value) {
            return value == null ? -1 : value.getValue();
        }

        @Override
        public final Year getValue(int code) {
            return code < 0 ? null : Year.of(code);
        }
    }



    /**
     * An IntCoding implementation for the Currency class.
     */
    class OfCurrency extends BaseCoding<Currency> implements IntCoding<Currency> {

        private static final long serialVersionUID = 1L;

        private final Currency[] currencies;
        private final Object2IntMap<Currency> codeMap;

        /**
         * Constructor
         */
        public OfCurrency() {
            super(Currency.class);
            this.currencies = Currency.getAvailableCurrencies().stream().toArray(Currency[]::new);
            this.codeMap = new Object2IntOpenHashMap<>(currencies.length, 0.5f);
            this.codeMap.defaultReturnValue(-1);
            Arrays.sort(currencies, (c1, c2) -> c1.getCurrencyCode().compareTo(c2.getCurrencyCode()));
            for (int i = 0; i< currencies.length; ++i) {
                this.codeMap.put(currencies[i], i);
            }
        }

        @Override
        public final int getCode(Currency value) {
            return value == null ? -1 : codeMap.getInt(value);
        }

        @Override
        public final Currency getValue(int code) {
            return code < 0 ? null : currencies[code];
        }
    }


    /**
     * An IntCoding implementation for the ZoneId class.
     */
    class OfZoneId extends BaseCoding<ZoneId> implements IntCoding<ZoneId> {

        private static final long serialVersionUID = 1L;

        private final ZoneId[] zoneIds;
        private final Object2IntMap<ZoneId> codeMap;

        /**
         * Constructor
         */
        OfZoneId() {
            super(ZoneId.class);
            this.zoneIds = ZoneId.getAvailableZoneIds().stream().map(ZoneId::of).toArray(ZoneId[]::new);
            this.codeMap = new Object2IntOpenHashMap<>(zoneIds.length, 0.5f);
            this.codeMap.defaultReturnValue(-1);
            Arrays.sort(zoneIds, (z1, z2) -> z1.getId().compareTo(z2.getId()));
            for (int i=0; i<zoneIds.length; ++i) {
                this.codeMap.put(zoneIds[i], i);
            }
        }

        @Override
        public final int getCode(ZoneId value) {
            return value == null ? -1 : codeMap.getInt(value);
        }

        @Override
        public final ZoneId getValue(int code) {
            return code < 0 ? null : zoneIds[code];
        }
    }


    /**
     * An IntCoding implementation for the TimeZone class.
     */
    class OfTimeZone extends BaseCoding<TimeZone> implements IntCoding<TimeZone> {

        private static final long serialVersionUID = 1L;

        private final TimeZone[] timeZones;
        private final Object2IntMap<TimeZone> codeMap;

        /**
         * Constructor
         */
        OfTimeZone() {
            super(TimeZone.class);
            this.timeZones = Arrays.stream(TimeZone.getAvailableIDs()).map(TimeZone::getTimeZone).toArray(TimeZone[]::new);
            this.codeMap = new Object2IntOpenHashMap<>(timeZones.length, 0.5f);
            this.codeMap.defaultReturnValue(-1);
            Arrays.sort(timeZones, (tz1, tz2) -> tz1.getID().compareTo(tz2.getID()));
            for (int i = 0; i< timeZones.length; ++i) {
                this.codeMap.put(timeZones[i], i);
            }
        }

        @Override
        public final int getCode(TimeZone value) {
            return value == null ? -1 : codeMap.getInt(value);
        }

        @Override
        public final TimeZone getValue(int code) {
            return code < 0 ? null : timeZones[code];
        }
    }


}
