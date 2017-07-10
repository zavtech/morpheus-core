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
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
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
 * An Array implementation containing mapped ZonedDateTime values stored as a longs of Epoch Millis.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class MappedArrayOfZonedDateTimes extends ArrayBase<ZonedDateTime> {

    private static final long serialVersionUID = 1L;

    private static final int BYTE_COUNT = 10;

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

    private File file;
    private int length;
    private FileChannel channel;
    private ByteBuffer byteBuffer;
    private long defaultValueAsLong;
    private short defaultZoneId;
    private ZonedDateTime defaultValue;


    /**
     * Constructor
     * @param length        the length for this array
     * @param defaultValue  the default value for array
     * @param file          the memory mapped file reference
     */
    MappedArrayOfZonedDateTimes(int length, ZonedDateTime defaultValue, File file) {
        super(ZonedDateTime.class, ArrayStyle.MAPPED, false);
        try {
            this.file = file;
            this.length = length;
            this.defaultValue = defaultValue;
            this.defaultValueAsLong = defaultValue != null ? defaultValue.toInstant().toEpochMilli() : nullValue;
            this.defaultZoneId = defaultValue != null ? zoneIdMap1.get(defaultValue.getZone()) : NULL_ZONE;
            this.channel = new RandomAccessFile(file, "rw").getChannel();
            this.byteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, BYTE_COUNT * length);
            this.fill(defaultValue);
        } catch (Exception ex) {
            throw new ArrayException("Failed to initialise memory mapped array on file: " + file.getAbsolutePath(), ex);
        }
    }

    /**
     * Constructor
     * @param source    the source array to shallow copy
     * @param parallel  true for the parallel version
     */
    private MappedArrayOfZonedDateTimes(MappedArrayOfZonedDateTimes source, boolean parallel) {
        super(source.type(), ArrayStyle.MAPPED, parallel);
        this.file = source.file;
        this.length = source.length;
        this.defaultValue = source.defaultValue;
        this.defaultValueAsLong = source.defaultValueAsLong;
        this.defaultZoneId = source.defaultZoneId;
        this.channel = source.channel;
        this.byteBuffer = source.byteBuffer;
    }


    @Override
    public final int length() {
        return length;
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
        return isParallel() ? this : new MappedArrayOfZonedDateTimes(this, true);
    }


    @Override
    public final Array<ZonedDateTime> sequential() {
        return isParallel() ? new MappedArrayOfZonedDateTimes(this, false) : this;
    }


    @Override()
    public final Array<ZonedDateTime> copy() {
        try {
            final File newFile = MappedArrayConstructor.randomFile(true);
            final MappedArrayOfZonedDateTimes copy = new MappedArrayOfZonedDateTimes(length, defaultValue, newFile);
            for (int i=0; i<length; ++i) {
                final int longIndex = i * BYTE_COUNT;
                final int shortIndex = i * BYTE_COUNT + 8;
                final long epochMillis = byteBuffer.getLong(longIndex);
                final short zoneId = byteBuffer.getShort(shortIndex);
                copy.byteBuffer.putLong(longIndex, epochMillis);
                copy.byteBuffer.putShort(shortIndex, zoneId);
            }
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed to copy Array: " + this, ex);
        }
    }


    @Override()
    public final Array<ZonedDateTime> copy(int[] indexes) {
        try {
            final File newFile = MappedArrayConstructor.randomFile(true);
            final MappedArrayOfZonedDateTimes copy = new MappedArrayOfZonedDateTimes(indexes.length, defaultValue, newFile);
            for (int i=0; i<indexes.length; ++i) {
                final int toIndex = i * BYTE_COUNT;
                final int fromIndex = indexes[i] * BYTE_COUNT;
                final long epochMillis = byteBuffer.getLong(fromIndex);
                final short zoneId = byteBuffer.getShort(fromIndex + 8);
                copy.byteBuffer.putLong(toIndex, epochMillis);
                copy.byteBuffer.putShort(toIndex + 8, zoneId);
            }
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed to copy Array: " + this, ex);
        }
    }


    @Override()
    public final Array<ZonedDateTime> copy(int start, int end) {
        try {
            final int newLength = end - start;
            final File newFile = MappedArrayConstructor.randomFile(true);
            final MappedArrayOfZonedDateTimes copy = new MappedArrayOfZonedDateTimes(newLength, defaultValue, newFile);
            for (int i=0; i<newLength; ++i) {
                final int toIndex = i * BYTE_COUNT;
                final int fromIndex = (start + i) * BYTE_COUNT;
                final long epochMillis = byteBuffer.getLong(fromIndex);
                final short zoneId = byteBuffer.getShort(fromIndex + 8);
                copy.byteBuffer.putLong(toIndex, epochMillis);
                copy.byteBuffer.putShort(toIndex + 8, zoneId);
            }
            return copy;
        } catch (Exception ex) {
            throw new ArrayException("Failed top copy subset of Array", ex);
        }
    }


    @Override
    protected final Array<ZonedDateTime> sort(int start, int end, int multiplier) {
        return doSort(start, end, (i, j) -> {
            final long v1 = byteBuffer.getLong(i * BYTE_COUNT);
            final long v2 = byteBuffer.getLong(j * BYTE_COUNT);
            return multiplier * Long.compare(v1, v2);
        });
    }


    @Override
    public final int compare(int i, int j) {
        final long v1 = byteBuffer.getLong(i * BYTE_COUNT);
        final long v2 = byteBuffer.getLong(j * BYTE_COUNT);
        return Long.compare(v1, v2);
    }


    @Override
    public final Array<ZonedDateTime> swap(int i, int j) {
        final int x = i * BYTE_COUNT;
        final int y = j * BYTE_COUNT;
        final long v1 = byteBuffer.getLong(x);
        final long v2 = byteBuffer.getLong(y);
        final short z1 = byteBuffer.getShort(x + 8);
        final short z2 = byteBuffer.getShort(y + 8);
        this.byteBuffer.putLong(x, v2);
        this.byteBuffer.putLong(y, v1);
        this.byteBuffer.putShort(x + 8, z2);
        this.byteBuffer.putShort(y + 8, z1);
        return this;
    }


    @Override
    public final Array<ZonedDateTime> filter(Predicate<ArrayValue<ZonedDateTime>> predicate) {
        final ArrayCursor<ZonedDateTime> cursor = cursor();
        final ArrayBuilder<ZonedDateTime> builder = ArrayBuilder.of(length(), type());
        for (int i=0; i<length; ++i) {
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
            if (from instanceof MappedArrayOfZonedDateTimes) {
                final MappedArrayOfZonedDateTimes other = (MappedArrayOfZonedDateTimes)from;
                for (int i=0; i<fromIndexes.length; ++i) {
                    final int toIndex = toIndexes[i] * BYTE_COUNT;
                    final int fromIndex = fromIndexes[i] * BYTE_COUNT;
                    this.byteBuffer.putLong(toIndex, other.byteBuffer.getLong(fromIndex));
                    this.byteBuffer.putShort(toIndex + 8, byteBuffer.getShort(fromIndex + 8));
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
        if (from instanceof MappedArrayOfZonedDateTimes) {
            final MappedArrayOfZonedDateTimes other = (MappedArrayOfZonedDateTimes)from;
            for (int i=0; i<length; ++i) {
                final int x = toIndex * BYTE_COUNT;
                final int y = fromIndex * BYTE_COUNT;
                this.byteBuffer.putLong(x, other.byteBuffer.getLong(y));
                this.byteBuffer.putShort(x + 8, byteBuffer.getShort(y + 8));
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
        try {
            if (newLength > length) {
                this.byteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, BYTE_COUNT * newLength);
                this.fill(defaultValue, length, newLength);
                this.length = newLength;
            }
            return this;
        } catch (Exception ex) {
            throw new ArrayException("Failed to expand size of memory mapped array at " + file.getAbsolutePath(), ex);
        }
    }


    @Override()
    public final Array<ZonedDateTime> fill(ZonedDateTime value) {
        final long fillEpochMillis = value == null ? nullValue : value.toInstant().toEpochMilli();
        final short fillZoneId = value == null ? NULL_ZONE : zoneIdMap1.get(value.getZone());
        for (int i=0; i<length; ++i) {
            final int index = i * BYTE_COUNT;
            this.byteBuffer.putLong(index, fillEpochMillis);
            this.byteBuffer.putShort(index + 8, fillZoneId);
        }
        return this;
    }


    @Override
    public Array<ZonedDateTime> fill(ZonedDateTime value, int start, int end) {
        final long fillEpochMillis = value == null ? nullValue : value.toInstant().toEpochMilli();
        final short fillZoneId = value == null ? NULL_ZONE : zoneIdMap1.get(value.getZone());
        for (int i=start; i<end; ++i) {
            final int index = i * BYTE_COUNT;
            this.byteBuffer.putLong(index, fillEpochMillis);
            this.byteBuffer.putShort(index + 8, fillZoneId);
        }
        return this;
    }


    @Override
    public boolean isNull(int index) {
        return byteBuffer.getLong(index * BYTE_COUNT) == nullValue;
    }


    @Override
    public final boolean isEqualTo(int index, ZonedDateTime value) {
        final long epochMillis = byteBuffer.getLong(index * BYTE_COUNT);
        if (value == null) {
            return epochMillis == nullValue;
        } else {
            final long valueAsEpochMills = value.toInstant().toEpochMilli();
            if (epochMillis == valueAsEpochMills) {
                return false;
            } else {
                final ZoneId zoneId = value.getZone();
                final short code1 = zoneIdMap1.get(zoneId);
                final short code2 = byteBuffer.getShort(index * BYTE_COUNT + 8);
                return code1 == code2;
            }
        }
    }


    @Override
    public final long getLong(int index) {
        this.checkBounds(index, length);
        return byteBuffer.getLong(index * BYTE_COUNT);
    }


    @Override
    @SuppressWarnings("unchecked")
    public final ZonedDateTime getValue(int index) {
        this.checkBounds(index, length);
        final int byteIndex = index * BYTE_COUNT;
        final long value = byteBuffer.getLong(byteIndex);
        if (value == nullValue) {
            return null;
        } else {
            final short zoneId = byteBuffer.getShort(byteIndex + 8);
            final ZoneId zone = zoneIdMap2.get(zoneId);
            final Instant instant = Instant.ofEpochMilli(value);
            return ZonedDateTime.ofInstant(instant, zone);
        }
    }


    @Override
    public final long setLong(int index, long value) {
        this.checkBounds(index, length);
        final int byteIndex = index * BYTE_COUNT;
        final long oldMillis = byteBuffer.getLong(byteIndex);
        final short oldZone = byteBuffer.getShort(byteIndex + 8);
        this.byteBuffer.putLong(byteIndex, value);
        if (oldZone < 0) {
            this.byteBuffer.putShort(byteIndex + 8, UTC_ZONE);
        }
        return oldMillis;
    }


    @Override
    public final ZonedDateTime setValue(int index, ZonedDateTime value) {
        this.checkBounds(index, length);
        final int byteIndex = index * BYTE_COUNT;
        final ZonedDateTime oldValue = getValue(index);
        if (value == null) {
            this.byteBuffer.putLong(byteIndex, nullValue);
            this.byteBuffer.putShort(byteIndex + 8, NULL_ZONE);
            return oldValue;
        } else  {
            this.byteBuffer.putLong(byteIndex, value.toInstant().toEpochMilli());
            this.byteBuffer.putShort(byteIndex + 8, zoneIdMap1.get(value.getZone()));
            return oldValue;
        }
    }


    @Override
    public int binarySearch(int start, int end, ZonedDateTime value) {
        try {
            int low = start;
            int high = end - 1;
            final long epochMillis = value != null ? value.toInstant().toEpochMilli() : Long.MIN_VALUE;
            while (low <= high) {
                final int midIndex = (low + high) >>> 1;
                final long midValue = byteBuffer.getLong(midIndex * BYTE_COUNT);
                final int result = Long.compare(midValue, epochMillis);
                if (result < 0) {
                    low = midIndex + 1;
                } else if (result > 0) {
                    high = midIndex - 1;
                } else {
                    return midIndex;
                }
            }
            return -(low + 1);
        } catch (Exception ex) {
            throw new ArrayException("Binary search of array failed", ex);
        }
    }


    @Override
    public final void read(ObjectInputStream is, int count) throws IOException {
        for (int i=0; i<count; ++i) {
            final int byteIndex = i * BYTE_COUNT;
            final long epochMillis = is.readLong();
            final short zoneId = is.readShort();
            this.byteBuffer.putLong(byteIndex, epochMillis);
            this.byteBuffer.putShort(byteIndex + 8, zoneId);
        }
    }


    @Override
    public final void write(ObjectOutputStream os, int[] indexes) throws IOException {
        for (int index : indexes) {
            final int byteIndex = index * BYTE_COUNT;
            final long epochMillis = byteBuffer.getLong(byteIndex);
            final short zoneId = byteBuffer.getShort(byteIndex + 8);
            os.writeLong(epochMillis);
            os.writeShort(zoneId);
        }
    }

    /** Custom serialization */
    private void writeObject(ObjectOutputStream os) throws IOException {
        os.writeInt(length);
        os.writeLong(defaultValueAsLong);
        os.writeShort(defaultZoneId);
        os.writeObject(defaultValue);
        for (int i=0; i<length; ++i) {
            final int byteIndex = i * BYTE_COUNT;
            final long epochMillis = byteBuffer.getLong(byteIndex);
            final short zoneId = byteBuffer.getShort(byteIndex + 8);
            os.writeLong(epochMillis);
            os.writeShort(zoneId);
        }
    }


    @SuppressWarnings("unchecked")
    /** Custom serialization */
    private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
        this.file = MappedArrayConstructor.randomFile(true);
        this.length = is.readInt();
        this.defaultValueAsLong = is.readLong();
        this.defaultZoneId = is.readShort();
        this.defaultValue = (ZonedDateTime)is.readObject();
        this.channel = new RandomAccessFile(file, "rw").getChannel();
        this.byteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, BYTE_COUNT * length);
        for (int i=0; i<length; ++i) {
            final int byteIndex = i * BYTE_COUNT;
            final long epochMillis = is.readLong();
            final short zoneId = is.readShort();
            this.byteBuffer.putLong(byteIndex, epochMillis);
            this.byteBuffer.putShort(byteIndex + 8, zoneId);
        }
    }

}
