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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Comparator;
import java.util.function.Predicate;

/**
 * A ready-only wrapper for a Morpheus Array inorder to expose an immutable view on an Array.
 *
 * @param <T>   the array element type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class ArrayReadOnly<T> extends ArrayBase<T> {

    private static final long serialVersionUID = 1L;

    private Array<T> source;

    /**
     * Constructor
     * @param source    the array to wrap with a ready-only interface
     */
    ArrayReadOnly(Array<T> source) {
        super(source.type(), source.style(), source.isParallel());
        this.source = source;
    }

    @Override
    public final int length() {
        return source.length();
    }

    @Override
    public float loadFactor() {
        return source.loadFactor();
    }

    @Override
    public final T defaultValue() {
        return source.defaultValue();
    }

    @Override
    public final boolean isReadOnly() {
        return true;
    }

    @Override
    public final Array<T> parallel() {
        return isParallel() ? this : new ArrayReadOnly<>(source.parallel());
    }

    @Override
    public final Array<T> sequential() {
        return isParallel() ? new ArrayReadOnly<>(source.sequential()) : this;
    }

    @Override
    public final Array<T> readOnly() {
        return this;
    }

    @Override
    public final Array<T> copy() {
        return source.copy().readOnly();
    }

    @Override
    public final Array<T> copy(int[] indexes) {
        return source.copy(indexes).readOnly();
    }

    @Override
    public final Array<T> copy(int start, int end) {
        return source.copy(start, end).readOnly();
    }

    @Override
    public final Array<T> fill(T value) {
        throw new ArrayException("This is a ready only Morpheus Array");
    }

    @Override
    public final Array<T> fill(T value, int start, int end) {
        throw new ArrayException("This is a ready only Morpheus Array");
    }

    @Override
    public final Array<T> shuffle(int count) {
        throw new ArrayException("This is a ready only Morpheus Array");
    }

    @Override
    public final int compare(int i, int j) {
        return source.compare(i, j);
    }

    @Override
    public final Array<T> swap(int i, int j) {
        throw new ArrayException("This is a ready only Morpheus Array");
    }

    @Override
    public final Array<T> sort(boolean ascending) {
        throw new ArrayException("This is a ready only Morpheus Array");
    }

    @Override
    public final Array<T> sort(int start, int end, boolean ascending) {
        throw new ArrayException("This is a ready only Morpheus Array");
    }

    @Override
    public final Array<T> sort(int start, int end, Comparator<T> comparator) {
        throw new ArrayException("This is a ready only Morpheus Array");
    }

    @Override
    public final Array<T> filter(Predicate<T> predicate) {
        return source.filter(predicate);
    }

    @Override
    public final Array<T> expand(int newLength) {
        throw new ArrayException("This is a ready only Morpheus Array");
    }

    @Override
    public final Array<T> update(Array<T> from, int[] fromIndexes, int[] toIndexes) {
        throw new ArrayException("This is a ready only Morpheus Array");
    }

    @Override
    public final Array<T> update(int toIndex, Array<T> from, int fromIndex, int length) {
        throw new ArrayException("This is a ready only Morpheus Array");
    }

    @Override
    public final boolean isNull(int index) {
        return source.isNull(index);
    }

    @Override
    public final boolean isEqualTo(int index, T value) {
        return source.isEqualTo(index, value);
    }

    @Override
    public final boolean getBoolean(int index) {
        return source.getBoolean(index);
    }

    @Override
    public final int getInt(int index) {
        return source.getInt(index);
    }

    @Override
    public final long getLong(int index) {
        return source.getLong(index);
    }

    @Override
    public final double getDouble(int index) {
        return source.getDouble(index);
    }

    @Override
    public final T getValue(int index) {
        return source.getValue(index);
    }

    @Override
    public final boolean setBoolean(int index, boolean value) {
        throw new ArrayException("This is a ready only Morpheus Array");
    }

    @Override
    public final int setInt(int index, int value) {
        throw new ArrayException("This is a ready only Morpheus Array");
    }

    @Override
    public final long setLong(int index, long value) {
        throw new ArrayException("This is a ready only Morpheus Array");
    }

    @Override
    public final double setDouble(int index, double value) {
        throw new ArrayException("This is a ready only Morpheus Array");
    }

    @Override
    public final T setValue(int index, T value) {
        throw new ArrayException("This is a ready only Morpheus Array");
    }

    @Override
    public final void read(ObjectInputStream is, int count) throws IOException {
        source.read(is, count);
    }

    @Override
    public final void write(ObjectOutputStream os, int[] indexes) throws IOException {
        source.write(os, indexes);
    }
}
