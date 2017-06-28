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

import com.zavtech.morpheus.array.dense.DenseArrayConstructor;
import com.zavtech.morpheus.array.mapped.MappedArrayConstructor;
import com.zavtech.morpheus.array.sparse.SparseArrayConstructor;
import com.zavtech.morpheus.util.Asserts;

/**
 * A factory class that exposes various Constructor objects for creating dense, sparse and memory mapped Morpheus Arrays.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public abstract class ArrayFactory {

    private static Constructor dense = new DenseArrayConstructor();
    private static Constructor sparse = new SparseArrayConstructor();
    private static Constructor mapped = new MappedArrayConstructor();

    /**
     * Returns a reference to the dense array constructor
     * @return  the dense array factory
     */
    public static Constructor dense() {
        return dense;
    }

    /**
     * Returns a reference to the sparse array constructor
     * @return  the sparse array factory
     */
    public static Constructor sparse() {
        return sparse;
    }

    /**
     * Returns a reference to the memory mapped array constructor
     * @return  the memory mapped array factory
     */
    public static Constructor mapped() {
        return mapped;
    }

    /**
     * Sets the dense array constructor
     * @param dense dense array constructor
     */
    public static void setDense(Constructor dense) {
        Asserts.notNull(dense, "The array constructor cannot be null");
        ArrayFactory.dense = dense;
    }

    /**
     * Sets the sparse array constructor
     * @param sparse sparse array constructor
     */
    public static void setSparse(Constructor sparse) {
        Asserts.notNull(dense, "The array constructor cannot be null");
        ArrayFactory.sparse = sparse;
    }

    /**
     * Sets the memory mapped array constructor
     * @param mapped memory mapped array constructor
     */
    public static void setMapped(Constructor mapped) {
        Asserts.notNull(dense, "The array constructor cannot be null");
        ArrayFactory.mapped = mapped;
    }

    /**
     * Returns a newly created Morpheus Array containing the array of values specified
     * @param array     an array of values to wrap in a Morpheus array
     * @return          the newly created array
     */
    @SuppressWarnings("unchecked")
    static <T> Array<T> create(Object array) {
        if (array == null) {
            throw new IllegalArgumentException("The array argument cannot be null");
        } else if (!array.getClass().isArray()) {
            throw new IllegalArgumentException("The argument must be an array");
        } else {
            final int length = java.lang.reflect.Array.getLength(array);
            final Class<T> arrayClass = resolveType(array);
            final T defaultValue = ArrayType.defaultValue(arrayClass);
            final Array<T> mArray = dense().apply(arrayClass, length, defaultValue, null);
            if (arrayClass == boolean.class) {
                final boolean[] booleans = (boolean[])array;
                return mArray.applyBooleans(v -> booleans[v.index()]);
            } else if (arrayClass == int.class) {
                final int[] ints = (int[])array;
                return mArray.applyInts(v -> ints[v.index()]);
            } else if (arrayClass == long.class) {
                final long[] longs = (long[])array;
                return mArray.applyLongs(v -> longs[v.index()]);
            } else if (arrayClass == double.class) {
                final double[] doubles = (double[])array;
                return mArray.applyDoubles(v -> doubles[v.index()]);
            } else {
                final Object[] objects = (Object[])array;
                return mArray.applyValues(v -> (T)objects[v.index()]);
            }
        }
    }


    /**
     * Returns a newly created array by concatenating the input arrays provided
     * @param type      the element type for new array
     * @param arrays    the input arrays to concatenate in order
     * @param <T>       the array element type
     * @return          the resulting array
     */
    static <T> Array<T> concat(Class<T> type, Iterable<Array<T>> arrays) {
        int startIndex = 0;
        int totalLength = 0;
        for (Array<T> array : arrays) {
            totalLength += array.length();
        }
        final Array<T> result = Array.of(type, totalLength);
        for (Array<T> array : arrays) {
            result.update(startIndex, array, 0, array.length());
            startIndex += array.length();
        }
        return result;
    }


    /**
     * Resolves the array type from the values provided
     * @param values    the array of values
     * @param <T>       the array type
     * @return          the array component type
     */
    @SuppressWarnings("unchecked")
    private static <T> Class<T> resolveType(Object values) {
        final int length = java.lang.reflect.Array.getLength(values);
        final Class<T> componentType = (Class<T>)values.getClass().getComponentType();
        if (length == 1 && componentType == Object.class) {
            final Object entry = java.lang.reflect.Array.get(values, 0);
            return entry != null ? (Class<T>)entry.getClass() : (Class<T>)Object.class;
        } else {
            return componentType;
        }
    }


    /**
     * An interface to a constructor of a specific style of array (dense, sparse, memory mapped)
     */
    public interface Constructor {

        /**
         * Returns a newly created array for type and with initial length and default value
         * @param type          the array element type
         * @param length        the initial length
         * @param defaultValue  the default value for array
         * @return              the newly created array
         */
        <T> Array<T> apply(Class<T> type, int length, T defaultValue);

        /**
         * Returns a newly created array for type and with initial length and default value
         * @param type          the array element type
         * @param length        the initial length
         * @param defaultValue  the default value for array
         * @param path          the file path, only used in memory mapped arrays
         * @return              the newly created array
         */
        <T> Array<T> apply(Class<T> type, int length, T defaultValue, String path);

    }
}
