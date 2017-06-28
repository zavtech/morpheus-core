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
package com.zavtech.morpheus.frame;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * An interface to access meta data about the header of a <code>DataFrame</code>
 *
 * @param <C>   the column key type
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameHeader<C> {

    /**
     * The number of entries in this header
     * @return  the header size
     */
    int size();

    /**
     * A stream of the column keys in the header
     * @return  the stream of column keys
     */
    Stream<C> keys();

    /**
     * Returns the data type for the column specified
     * @param colKey    the column key
     * @return          the data type for column
     */
    Class<?> type(C colKey);

    /**
     * Returns a newly created header based on the builder passed to the consumer
     * @param consumer  the consumer that receives a new builder instance
     * @param <C>       the column type
     * @return          the newly created header
     */
    static <C> DataFrameHeader<C> of(Consumer<Builder> consumer) {
        final DataFrameHeader.Builder<C> builder = new DataFrameHeader.Builder<>();
        consumer.accept(builder);
        return builder.build();
    }

    /**
     * Returns a header to describe the column with the specified properties
     * @param key           the column key
     * @param type          the data type for column
     * @param <C>           the column type
     * @return              the newly created header
     */
    static <C> DataFrameHeader<C> of(C key, Class<?> type) {
        return new DataFrameHeader<C>() {
            @Override
            public int size() {
                return 1;
            }
            @Override
            public Stream<C> keys() {
                return Stream.of(key);
            }
            @Override
            public Class<?> type(C colKey) {
                return type;
            }
        };
    }

    /**
     * Returns a header of columns of the same type with the characteristics specified
     * @param keys          the column keys
     * @param type          the data type
     * @param <C>           the column type
     * @return              the newly created header
     */
    static <C> DataFrameHeader<C> of(Collection<C> keys, Class<?> type) {
        return new DataFrameHeader<C>() {
            @Override
            public int size() {
                return keys.size();
            }
            @Override
            public Stream<C> keys() {
                return keys.stream();
            }
            @Override
            public Class<?> type(C colKey) {
                return type;
            }
        };
    }

    /**
     * Returns a filtered header with keys that match the predicate only
     * @param predicate the predicate to filter keys on
     * @return          the filtered header
     */
    default DataFrameHeader<C> filter(Predicate<C> predicate) {
        return new DataFrameHeader<C>() {
            @Override()
            public int size() {
                return (int)keys().count();
            }
            @Override
            public Stream<C> keys() {
                return DataFrameHeader.this.keys().filter(predicate);
            }
            @Override
            public Class<?> type(C colKey) {
                return DataFrameHeader.this.type(colKey);
            }
        };
    }


    /**
     * A builder to facilitate construction of a DataFrameHeader
     * @param <C>   the column key type
     */
    class Builder<C> {

        private Map<C,Class<?>> typeMap = new LinkedHashMap<>();

        /**
         * Adds a column definition to this builder
         * @param colKey    the column key
         * @param type      the data type
         * @return          this builder
         */
        public Builder<C> add(C colKey, Class<?> type) {
            this.typeMap.put(colKey, type);
            return this;
        }

        /**
         * Adds several columns to this builder of a specific type
         * @param colKeys   the column keys
         * @param type      the data type
         * @return          this builder
         */
        public Builder<C> add(C[] colKeys, Class<?> type) {
            for (C colKey : colKeys) {
                this.typeMap.put(colKey, type);
            }
            return this;
        }

        /**
         * Returns a newly create header based on this builder
         * @return      the newly created header
         */
        public DataFrameHeader<C> build() {
            return new DataFrameHeader<C>() {
                @Override
                public int size() {
                    return typeMap.size();
                }
                @Override
                public Stream<C> keys() {
                    return typeMap.keySet().stream();
                }
                @Override
                public Class<?> type(C colKey) {
                    return typeMap.get(colKey);
                }
            };
        }
    }
}
