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

import java.util.function.Function;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.stats.StatType;

/**
 * An interface that provides functions to operate on the row dimension of a DataFrame
 *
 * @see DataFrameOptions
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameRows<R,C> extends DataFrameAxis<R,C,R,C,DataFrameRow<R,C>,DataFrameRows<R,C>,DataFrameGrouping.Rows<R,C>> {

    /**
     * Adds a new row key to this axis if the key does not already exists
     * By default, ignores duplicates unless run within DataFrameOptions.whileNotIgnoringDuplicates()
     * @param key   the key to add
     * @return      true if a new row was added
     * @see DataFrameOptions
     */
    boolean add(R key) throws DataFrameException;

    /**
     * Adds a new row key to this axis if the key does not already exists
     * By default, ignores duplicates unless run within DataFrameOptions.whileNotIgnoringDuplicates()
     * @param key       the key to add
     * @param initials  the function that provides initial values, null permitted
     * @return          true if a new row was added
     * @see DataFrameOptions
     */
    boolean add(R key, Function<DataFrameValue<R,C>,?> initials);

    /**
     * Adds multiple row keys, ignoring any keys that already exist in the axis
     * By default, ignores duplicates unless run within DataFrameOptions.whileNotIgnoringDuplicates()
     * @param keys  the keys to add
     * @return      the keys added
     * @see DataFrameOptions
     */
    Array<R> addAll(Iterable<R> keys);

    /**
     * Adds multiple row keys, ignoring any keys that already exist in the axis
     * By default, ignores duplicates unless run within DataFrameOptions.whileNotIgnoringDuplicates()
     * @param keys      the keys to add
     * @param initials  a function to provide initial values for newly added rows
     * @return          the keys added
     * @see DataFrameOptions
     */
    Array<R> addAll(Iterable<R> keys, Function<DataFrameValue<R,C>,?> initials);

    /**
     * Returns a reference to the stats API for the row dimension
     * @return      the stats API to operate in the row dimension
     */
    DataFrameAxisStats<R,R,C,R,StatType> stats();

    /**
     * Returns a DataFrame containing one or more stats for the rows
     * @param stats     the sequence of stats to compute, none implies all stats
     * @return          the DataFrame of row statistics
     */
    DataFrame<R,StatType> describe(StatType... stats);

    /**
     * Maps row keys in place according to the mapper function
     * @param mapper    the mapper function to apply row keys
     * @param <X>       the new row key type
     * @return          this frame with a new row axis
     */
    <X> DataFrame<X,C> mapKeys(Function<DataFrameRow<R,C>,X> mapper);

}
