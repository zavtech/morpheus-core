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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import com.zavtech.morpheus.array.ArrayBuilder;
import com.zavtech.morpheus.array.ArrayCursor;
import com.zavtech.morpheus.array.ArrayException;
import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayBase;
import com.zavtech.morpheus.array.ArrayStyle;
import com.zavtech.morpheus.array.ArrayValue;

/**
 * An Array implementation containing a dense array of ZonedDateTime values stored as a longs of Epoch Millis.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class DenseArrayOfZonedDateTimes extends ArrayBase<ZonedDateTime> {

    private static final long serialVersionUID = 1L;

    private static final Map<ZoneId,Short> zoneIdMap1 = new HashMap<>();
    private static final Map<Short,ZoneId> zoneIdMap2 = new HashMap<>();

    /**
     * Static initializer
     */
    static {
        short counter = 0;
        final List<String> keys = new ArrayList<>();
        keys.add("Z");
        keys.addAll(ZoneId.getAvailableZoneIds());
        for (String key: keys) {
            final short index = ++counter;
            final ZoneId zoneId = ZoneId.of(key);
            zoneIdMap1.put(zoneId, index);
            zoneIdMap2.put(index, zoneId);
        }
    }

    private static final long nullValue = Long.MIN_VALUE;
    private static final short NULL_ZONE = -1;
    private static final short UTC_ZONE = zoneIdMap1.get(ZoneId.of("UTC"));

    private long[] values;
    private short[] zoneIds;
    private long defaultValueAsLong;
    private final short defaultZoneId;
    private ZonedDateTime defaultValue;

    /**
     * Constructor
     * @param length        the length for this array
     * @param defaultValue  the default value for array
     */
    DenseArrayOfZonedDateTimes(int length, ZonedDateTime defaultValue) {
        super(ZonedDateTime.class, ArrayStyle.DENSE, false);
        this.values = new long[length];
        this.zoneIds = new short[length];
        this.defaultValue = defaultValue;
        this.defaultValueAsLong = defaultValue != null ? defaultValue.toInstant().toEpochMilli() : nullValue;
        this.defaultZoneId = defaultValue != null ? zoneIdMap1.get(defaultValue.getZone()) : NULL_ZONE;
        Arrays.fill(values, defaultValueAsLong);
        Arrays.fill(zoneIds, defaultZoneId);
    }

    /**
     * Constructor
     * @param source    the source array to shallow copy
     * @param parallel  true for the parallel version
     */
    private DenseArrayOfZonedDateTimes(DenseArrayOfZonedDateTimes source, boolean parallel) {
        super(source.type(), ArrayStyle.DENSE, parallel);
        this.values = source.values;
        this.zoneIds = source.zoneIds;
        this.defaultValue = source.defaultValue;
        this.defaultValueAsLong = source.defaultValueAsLong;
        this.defaultZoneId = source.defaultZoneId;
    }


    @Override
    public final int length() {
        return values.length;
    }


    @Override
    public float loadFactor() {
        return 1F;
    }


    @Override
    public final ZonedDateTime defaultValue() {
        return defaultValue;
    }


    @Override
    public final Array<ZonedDateTime> parallel() {
        return isParallel() ? this : new DenseArrayOfZonedDateTimes(this, true);
    }


    @Override
    public final Array<ZonedDateTime> sequential() {
        return isParallel() ? new DenseArrayOfZonedDateTimes(this, false) : this;
    }


    @Override()
    public final Array<ZonedDateTime> copy() {
        try {
            final DenseArrayOfZonedDateTimes copy = (DenseArrayOfZonedDateTimes)super.clone();
            copy.defaultValue = this.defaultValue;
            copy.defaultValueAsLong = this.defaultValueAsLong;
            copy.values = this.values.clone();
            copy.zoneIds = this.zoneIds.clone();
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed to copy Array: " + this, ex);
        }
    }


    @Override()
    public final Array<ZonedDateTime> copy(int[] indexes) {
        final DenseArrayOfZonedDateTimes clone = new DenseArrayOfZonedDateTimes(indexes.length, defaultValue);
        for (int i = 0; i < indexes.length; ++i) {
            clone.values[i] = this.values[indexes[i]];
            clone.zoneIds[i] = this.zoneIds[indexes[i]];
        }
        return clone;
    }


    @Override()
    public final Array<ZonedDateTime> copy(int start, int end) {
        final int length = end - start;
        final DenseArrayOfZonedDateTimes clone = new DenseArrayOfZonedDateTimes(length, defaultValue);
        System.arraycopy(values, start, clone.values, 0, length);
        System.arraycopy(zoneIds, start, clone.zoneIds, 0, length);
        return clone;
    }


    @Override
    protected final Array<ZonedDateTime> sort(int start, int end, int multiplier) {
        return doSort(start, end, (i, j) -> multiplier * Long.compare(values[i], values[j]));
    }


    @Override
    public final int compare(int i, int j) {
        return Long.compare(values[i], values[j]);
    }


    @Override
    public final Array<ZonedDateTime> swap(int i, int j) {
        final long v1 = values[i];
        final long v2 = values[j];
        final short z1 = zoneIds[i];
        final short z2 = zoneIds[j];
        this.values[i] = v2;
        this.values[j] = v1;
        this.zoneIds[i] = z2;
        this.zoneIds[j] = z1;
        return this;
    }


    @Override
    public final Array<ZonedDateTime> filter(Predicate<ArrayValue<ZonedDateTime>> predicate) {
        final ArrayCursor<ZonedDateTime> cursor = cursor();
        final ArrayBuilder<ZonedDateTime> builder = ArrayBuilder.of(length(), type());
        for (int i=0; i<values.length; ++i) {
            cursor.moveTo(i);
            final boolean match = predicate.test(cursor);
            if (match) {
                builder.add(cursor.getValue());
            }
        }
        return builder.toArray();
    }


    @Override
    public final Array<ZonedDateTime> update(Array<ZonedDateTime> from, int[] fromIndexes, int[] toIndexes) {
        if (fromIndexes.length != toIndexes.length) {
            throw new ArrayException("The from index array must have the same length as the to index array");
        } else {
            if (from instanceof DenseArrayOfZonedDateTimes) {
                final DenseArrayOfZonedDateTimes other = (DenseArrayOfZonedDateTimes)from;
                for (int i=0; i<fromIndexes.length; ++i) {
                    final int toIndex = toIndexes[i];
                    final int fromIndex = fromIndexes[i];
                    this.values[toIndex] = other.values[fromIndex];
                    this.zoneIds[toIndex] = other.zoneIds[fromIndex];
                }
            } else {
                for (int i=0; i<fromIndexes.length; ++i) {
                    final int toIndex = toIndexes[i];
                    final int fromIndex = fromIndexes[i];
                    final ZonedDateTime update = from.getValue(fromIndex);
                    this.setValue(toIndex, update);
                }
            }
        }
        return this;
    }


    @Override
    public final Array<ZonedDateTime> update(int toIndex, Array<ZonedDateTime> from, int fromIndex, int length) {
        if (from instanceof DenseArrayOfZonedDateTimes) {
            final DenseArrayOfZonedDateTimes other = (DenseArrayOfZonedDateTimes)from;
            for (int i=0; i<length; ++i) {
                this.values[toIndex + i] = other.values[fromIndex + i];
                this.zoneIds[toIndex + i] = other.zoneIds[fromIndex + i];
            }
        } else {
            for (int i=0; i<length; ++i) {
                final ZonedDateTime update = from.getValue(fromIndex + i);
                this.setValue(toIndex + i, update);
            }
        }
        return this;
    }


    @Override
    public final Array<ZonedDateTime> expand(int newLength) {
        if (newLength > values.length) {
            final long[] newValues = new long[newLength];
            final short[] newZoneIds = new short[newLength];
            System.arraycopy(values, 0, newValues, 0, values.length);
            System.arraycopy(zoneIds, 0, newZoneIds, 0, zoneIds.length);
            Arrays.fill(newValues, values.length, newValues.length, defaultValueAsLong);
            Arrays.fill(newZoneIds, zoneIds.length, newZoneIds.length, defaultZoneId);
            this.values = newValues;
            this.zoneIds = newZoneIds;
        }
        return this;
    }


    @Override
    public Array<ZonedDateTime> fill(ZonedDateTime value, int start, int end) {
        Arrays.fill(values, start, end, value != null ? value.toInstant().toEpochMilli() : nullValue);
        Arrays.fill(zoneIds, value != null ? zoneIdMap1.get(value.getZone()) : NULL_ZONE);
        return this;
    }


    @Override
    public boolean isNull(int index) {
        return values[index] == nullValue;
    }


    @Override
    public final boolean isEqualTo(int index, ZonedDateTime value) {
        if (value == null) {
            return values[index] == nullValue;
        } else {
            final long epochMills = value.toInstant().toEpochMilli();
            if (epochMills != values[index]) {
                return false;
            } else {
                final ZoneId zoneId = value.getZone();
                final short code1 = zoneIdMap1.get(zoneId);
                final short code2 = zoneIds[index];
                return code1 == code2;
            }
        }
    }


    @Override
    public final long getLong(int index) {
        return values[index];
    }


    @Override
    @SuppressWarnings("unchecked")
    public final ZonedDateTime getValue(int index) {
        final long value = values[index];
        if (value == nullValue) {
            return null;
        } else {
            final ZoneId zone = zoneIdMap2.get(zoneIds[index]);
            final Instant instant = Instant.ofEpochMilli(value);
            return ZonedDateTime.ofInstant(instant, zone);
        }
    }


    @Override
    public final long setLong(int index, long value) {
        final long oldValue = getLong(index);
        this.values[index] = value;
        this.zoneIds[index] = zoneIds[index] >= 0 ? zoneIds[index] : UTC_ZONE;
        return oldValue;
    }


    @Override
    public final ZonedDateTime setValue(int index, ZonedDateTime value) {
        final ZonedDateTime oldValue = getValue(index);
        if (value == null) {
            this.values[index] = nullValue;
            this.zoneIds[index] = NULL_ZONE;
            return oldValue;
        } else  {
            this.values[index] = value.toInstant().toEpochMilli();
            this.zoneIds[index] = zoneIdMap1.get(value.getZone());
            return oldValue;
        }
    }


    @Override
    public int binarySearch(int start, int end, ZonedDateTime value) {
        return Arrays.binarySearch(values, start, end, value.toInstant().toEpochMilli());
    }


    @Override
    public final void read(ObjectInputStream is, int count) throws IOException {
        for (int i=0; i<count; ++i) {
            final long value = is.readLong();
            this.values[i] = value;
            if (value != defaultValueAsLong) {
                this.zoneIds[i] = is.readShort();
            }
        }
    }


    @Override
    public final void write(ObjectOutputStream os, int[] indexes) throws IOException {
        for (int index : indexes) {
            final long value = values[index];
            os.writeLong(value);
            if (value != defaultValueAsLong) {
                os.writeShort(zoneIds[index]);
            }
        }
    }

    /** Custom serialization */
    private void writeObject(ObjectOutputStream os) throws IOException {
        final int length = values.length;
        os.writeInt(length);
        os.writeLong(defaultValueAsLong);
        os.writeObject(defaultValue);
        for (int i=0; i<length; ++i) {
            os.writeLong(values[i]);
            os.writeShort(zoneIds[i]);
        }
    }


    @SuppressWarnings("unchecked")
    /** Custom serialization */
    private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
        final int length = is.readInt();
        this.values = new long[length];
        this.zoneIds = new short[length];
        this.defaultValueAsLong = is.readLong();
        this.defaultValue = (ZonedDateTime)is.readObject();
        for (int i=0; i<length; ++i) {
            values[i] = is.readLong();
            zoneIds[i] = is.readShort();
        }
    }


}
