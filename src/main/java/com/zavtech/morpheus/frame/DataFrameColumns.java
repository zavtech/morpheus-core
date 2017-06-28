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

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.stats.StatType;

/**
 * An interface that provides functions to operate on the column dimension of a DataFrame
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameColumns<R,C> extends DataFrameAxis<C,R,R,C,DataFrameColumn<R,C>,DataFrameColumns<R,C>,DataFrameGrouping.Cols<R,C>> {

    /**
     * Adds a column if one does not already exist for the key specified
     * @param key       the column key
     * @param type      the data type for the column
     * @return          a reference to the column for the key specified
     */
    DataFrameColumn<R,C> add(C key, Class<?> type);

    /**
     * Adds a column if one does not already exist with the values provided
     * @param key       the column key
     * @param values    the values for column
     * @return          a reference to the column for the key specified
     */
    DataFrameColumn<R,C> add(C key, Iterable<?> values);

    /**
     * Adds a column if one does not already exist for the key specified
     * @param key       the column key
     * @param type      the data type for the column
     * @param seed      the function that seeds values for the column
     * @return          a reference to the column for the key specified
     */
    <T> DataFrame<R,C> add(C key, Class<T> type, Function<DataFrameValue<R,C>,T> seed);

    /**
     * Adds columns to this frame based on the column key and value mapping
     * @param consumer  the consumer that populates the map with key array mappings
     * @return          the column keys of newly added columns
     */
    Array<C> addAll(Consumer<Map<C,Iterable<?>>> consumer);

    /**
     * Adds multiple columns if they do not already exist
     * @param colKeys   the column keys to add
     * @param type      the data type for columns
     * @return          the column keys of newly added columns
     */
    Array<C> addAll(Iterable<C> colKeys, Class<?> type);

    /**
     * Returns a reference to the stats API for the column dimension
     * @return      the stats API to operate in the column dimension
     */
    DataFrameAxisStats<C,R,C,C,StatType> stats();

    /**
     * Returns a DataFrame containing one or more stats for the rows
     * @param stats     the sequence of stats to compute, none implies all stats
     * @return          the DataFrame of row statistics
     */
    DataFrame<C,StatType> describe(StatType... stats);

    /**
     * Returns a new shallow copy of the frame with the mapped column keys
     * @param mapper    the mapper function to map column keys
     * @param <X>       the new column key type
     * @return          a shallow copy of the frame with new keys
     */
    <X> DataFrame<R,X> mapKeys(Function<DataFrameColumn<R,C>, X> mapper);

    /**
     * Returns a DataFrame with evenly distributed frequency counts for the columns specified
     * @param binCount  the number of bins to include in frequency distribution
     * @param columns   the column keys to generate frequency distribution
     * @return          the DataFrame histogram with frequency distributions
     */
    DataFrame<Double,C> hist(int binCount, C... columns);

    /**
     * Returns a DataFrame with evenly distributed frequency counts for the columns specified
     * @param binCount  the number of bins to include in frequency distribution
     * @param columns   the column keys to generate frequenct distribution
     * @return          the DataFrame histogram with frequency distributions
     */
    DataFrame<Double,C> hist(int binCount, Iterable<C> columns);

}
