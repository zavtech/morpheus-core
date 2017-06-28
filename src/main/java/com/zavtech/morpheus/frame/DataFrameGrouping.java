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

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import com.zavtech.morpheus.stats.Stats;
import com.zavtech.morpheus.util.Tuple;

/**
 * An interface to a grouping of <code>DataFrames</code> providing various aggregation functions
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameGrouping<R,C,S> {

    /**
     * A grouping interface specialization that represents a grouping of rows
     * @param <R>       the row key type
     * @param <C>       the column key type
     */
    interface Rows<R,C> extends DataFrameGrouping<R,C,DataFrame<Tuple,C>> {}

    /**
     * A grouping interface specialization that represents a grouping of columns
     * @param <R>       the row key type
     * @param <C>       the column key type
     */
    interface Cols<R,C> extends DataFrameGrouping<R,C,DataFrame<R,Tuple>> {}

    /**
     * Returns the source frame for this grouping
     * @return      the source frame for groupings
     */
    DataFrame<R,C> source();

    /**
     * Returns the number of grouped levels
     * @return      the number of grouped levels
     */
    int getDepth();

    /**
     * Returns the number of groups for the level specified
     * @param level     level 0 implies top level, 1 implies level below that, and so on
     * @return  the group count for level
     */
    int getGroupCount(int level);

    /**
     * Returns the stats interface for groups at the specified level
     * @param level     the level for group stats
     * @return          the group stats
     */
    Stats<S> stats(int level);

    /**
     * Returns a stream over the groups keys for the level specified
     * @param level     level 0 implies top level, 1 implies level below that, and so on
     * @return  the group key stream
     */
    Stream<Tuple> getGroupKeys(int level);

    /**
     * Returns the stream of immediate child groups for the argument
     * @param groupKey  the group key for which to select immedicate children
     * @return          the stream of child groups
     */
    Stream<Tuple> getChildren(Tuple groupKey);

    /**
     * Returns the parent group key if the argument has level > 0
     * @param groupKey  the group key to compute parent from
     * @return          the parent group key if applicable
     */
    Optional<Tuple> getParent(Tuple groupKey);

    /**
     * Returns the frame for the group specified
     * @param groupKey  the group key
     * @return          the frame for the group key
     */
    DataFrame<R,C> getGroup(Tuple groupKey);

    /**
     * A forEach implementation that processes each group
     * @param level     level 0 implies top level, 1 implies level below that, and so on
     * @param groupConsumer  the consumer to receive each group
     */
    void forEach(int level, BiConsumer<Tuple,DataFrame<R,C>> groupConsumer);

}
