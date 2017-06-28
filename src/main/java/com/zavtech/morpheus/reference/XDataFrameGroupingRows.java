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
package com.zavtech.morpheus.reference;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayBuilder;
import com.zavtech.morpheus.array.ArrayCollector;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameGrouping;
import com.zavtech.morpheus.frame.DataFrameOptions;
import com.zavtech.morpheus.frame.DataFrameRow;
import com.zavtech.morpheus.frame.DataFrameVector;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.stats.Statistic1;
import com.zavtech.morpheus.stats.Stats;
import com.zavtech.morpheus.stats.StatsAssembler;
import com.zavtech.morpheus.util.Tuple;

/**
 * An implementation of the DataFrameGrouping interface that groups rows of a DataFrame
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class XDataFrameGroupingRows<R,C> implements DataFrameGrouping.Rows<R,C> {

    private int depth;
    private XDataFrame<R,C> source;
    private Map<Tuple,Array<R>> groupKeysMap;

    /**
     * Constructor
     * @param source        the source frame for groups
     * @param depth         the grouping depth
     * @param groupKeysMap  the apply of keys for each group
     */
    private XDataFrameGroupingRows(XDataFrame<R,C> source, int depth, Map<Tuple,Array<R>> groupKeysMap) {
        this.source = source;
        this.depth = depth;
        this.groupKeysMap = groupKeysMap;
    }


    /**
     * Returns a new grouping engine by rows based on values in the specified columns
     * @param source    the source frame to group
     * @param parallel  true for parallel grouping
     * @param colKeys   the column keys to group by
     * @param <R>       the row key type
     * @param <C>       the column key type
     * @return          the newly create grouping engine
     */
    static <R,C> XDataFrameGroupingRows<R,C> of(XDataFrame<R,C> source, boolean parallel, Array<C> colKeys) {
        if (colKeys.length() == 1) {
            final C colKey = colKeys.getValue(0);
            final int ordinal = source.colKeys().getOrdinalForKey(colKey);
            return XDataFrameGroupingRows.of(source, parallel, row -> Tuple.of(row.<Object>getValue(ordinal)));
        } else {
            final int[] ordinals = source.colKeys().ordinals(colKeys).toArray();
            return XDataFrameGroupingRows.of(source, parallel, row -> {
                final Object[] values = new Object[colKeys.length()];
                for (int i=0; i<ordinals.length; ++i) {
                    values[i] = row.getValue(ordinals[i]);
                }
                return Tuple.of(values);
            });
        }
    }

    /**
     * Returns a new grouping engine by rows
     * @param source    the source frame to group
     * @param function  the grouping function
     * @param <R>       the row key
     * @param <C>       the column key
     * @return          the newly create grouping engine
     */
    static <R,C> XDataFrameGroupingRows<R,C> of(XDataFrame<R,C> source, boolean parallel, Function<DataFrameRow<R,C>,Tuple> function) {
        final int depth = source.rows().first().map(function).map(Tuple::size).orElse(0);
        final GroupRowsTask<R,C> task = new GroupRowsTask<>(source, 0, source.rowCount()-1, depth, parallel, function);
        if (parallel) {
            final Map<Tuple,ArrayBuilder<R>> groupKeyMap = ForkJoinPool.commonPool().invoke(task);
            return new XDataFrameGroupingRows<>(source, depth, crystallize(groupKeyMap));
        } else {
            final Map<Tuple,ArrayBuilder<R>> groupKeyMap = task.compute();
            return new XDataFrameGroupingRows<>(source, depth, crystallize(groupKeyMap));
        }
    }


    @Override
    public final DataFrame<R,C> source() {
        return source;
    }

    @Override
    public final int getDepth() {
        return depth;
    }

    @Override
    public final Stats<DataFrame<Tuple,C>> stats(int level) {
        return new GroupedRowStats(level);
    }

    @Override
    public final int getGroupCount(int level) {
        return (int)groupKeysMap.keySet().stream().filter(g -> g.size() == level + 1).count();
    }

    @Override
    public final Stream<Tuple> getGroupKeys(int level) {
        return groupKeysMap.keySet().stream().filter(g -> g.size() == level + 1);
    }

    @Override
    public final Optional<Tuple> getParent(Tuple groupKey) {
        return groupKey.size() <= 1 ? Optional.empty() : Optional.of(groupKey.filter(0, groupKey.size()-1));
    }

    @Override
    public final Stream<Tuple> getChildren(Tuple groupKey) {
        return groupKeysMap.keySet().stream().filter(g -> g.size() == groupKey.size() + 1 && g.filter(0, groupKey.size()).equals(groupKey));
    }

    @Override
    @SuppressWarnings("unchecked")
    public final DataFrame<R,C> getGroup(Tuple groupKey) {
        final Array<R> groupKeys = groupKeysMap.get(groupKey);
        if (groupKeys == null) {
            throw new DataFrameException("No DataFrame for group " + groupKey);
        } else {
            final Index<R> rowKeys = source.rowKeys().filter(groupKeys);
            final Index<C> colKeys = source.colKeys().readOnly();
            return source.filter(rowKeys, colKeys);
        }
    }

    @Override
    public final void forEach(int level, BiConsumer<Tuple,DataFrame<R,C>> groupConsumer) {
        this.getGroupKeys(level).forEach(groupKey -> {
            try {
                final DataFrame<R,C> group = getGroup(groupKey);
                groupConsumer.accept(groupKey, group);
            } catch (Exception ex) {
                throw new DataFrameException("Failed to process group for key: " + groupKey, ex);
            }
        });
    }


    /**
     * Returns a apply with the values of the input apply crystallized to Morpheus arrays
     * @param input     the input apply for which to crystallize the values
     * @param <K>       the key type
     * @return          the crystallized apply
     */
    private static <K> Map<Tuple,Array<K>> crystallize(Map<Tuple,ArrayBuilder<K>> input) {
        final Map<Tuple,Array<K>> output = new HashMap<>(input.size());
        input.forEach((key, value) -> output.put(key, value.toArray()));
        return output;
    }


    /**
     * A RecursiveTask implementation to group a DataFrame along the row dimension
     */
    private static class GroupRowsTask<X,Y> extends RecursiveTask<Map<Tuple,ArrayBuilder<X>>> {

        private int from;
        private int to;
        private int depth;
        private boolean parallel;
        private XDataFrame<X,Y> source;
        private int threshold = Integer.MAX_VALUE;
        private Function<DataFrameRow<X,Y>,Tuple> function;

        /**
         * Constructor
         * @param source    the source frame to group
         * @param from      the from index (inclusive)
         * @param to        the to index (inclusive)
         * @param depth     the grouping depth
         * @param parallel  true for parallel mode
         * @param function  the row grouping function
         */
        private GroupRowsTask(XDataFrame<X,Y> source, int from, int to, int depth, boolean parallel, Function<DataFrameRow<X,Y>,Tuple> function) {
            this.source = source;
            this.from = from;
            this.to = to;
            this.depth = depth;
            this.parallel = parallel;
            this.function = function;
            if (parallel) {
                this.threshold = DataFrameOptions.getRowSplitThreshold(source);
            }
        }

        @Override
        protected Map<Tuple,ArrayBuilder<X>> compute() {
            final int count = to - from + 1;
            if (count > threshold) {
                return split();
            } else {
                final Class<X> keyType = source.rows().keyType();
                final Map<Tuple,ArrayBuilder<X>> groupKeyMap = new HashMap<>();
                final XDataFrameRow<X,Y> row = new XDataFrameRow<>(source, false);
                for (int i=from; i<=to; ++i) {
                    row.moveTo(i);
                    final X rowKey = row.key();
                    try {
                        final Tuple groupKey = function.apply(row);
                        for (int level=0; level<depth; ++level) {
                            final Tuple tuple = groupKey.filter(0, level + 1);
                            ArrayBuilder<X> groupKeyBuilder = groupKeyMap.get(tuple);
                            if (groupKeyBuilder == null) {
                                groupKeyBuilder = ArrayBuilder.of(1000, keyType);
                                groupKeyMap.put(tuple, groupKeyBuilder);
                            }
                            groupKeyBuilder.add(rowKey);
                        }
                    } catch (Exception ex) {
                        throw new DataFrameException("Grouping failed at row: " + rowKey, ex);
                    }
                }
                return groupKeyMap;
            }
        }

        /**
         * Splits into two grouping operations and then combines the results of each
         * @return      the combined grouping of the two split grouping tasks
         */
        private Map<Tuple,ArrayBuilder<X>> split() {
            final int splitCount = (to - from) / 2;
            final int midPoint = from + splitCount;
            final GroupRowsTask<X,Y> left  = new GroupRowsTask<>(source, from, midPoint, depth, parallel, function);
            final GroupRowsTask<X,Y> right = new GroupRowsTask<>(source, midPoint + 1, to, depth, parallel, function);
            left.fork();
            final Map<Tuple,ArrayBuilder<X>> rightAns = right.compute();
            final Map<Tuple,ArrayBuilder<X>> leftAns  = left.join();
            leftAns.forEach((key, value) -> {
                final ArrayBuilder<X> existing = rightAns.get(key);
                if (existing == null) {
                    rightAns.put(key, value);
                } else {
                    existing.addAll(value);
                }
            });
            return rightAns;
        }
    }


    /**
     * Computes bulk statistics over grouped rows
     */
    private class GroupedRowStats extends StatsAssembler<DataFrame<Tuple,C>> {

        private int level;

        /**
         * Constructor
         * @param level the group level
         */
        GroupedRowStats(int level) {
            this.level = level;
        }

        @Override()
        protected DataFrame<Tuple,C> compute(Statistic1 stat) {
            try {
                final int groupCount = getGroupCount(level);
                final Array<Tuple> groupKeys = getGroupKeys(level).collect(ArrayCollector.of(Tuple.class, groupCount));
                final Array<C> columnKeys = source.cols().filter(DataFrameVector::isNumeric).keyArray();
                final DataFrame<Tuple,C> result = DataFrame.ofDoubles(groupKeys, columnKeys);
                result.rows().forEach(row -> {
                    final Tuple groupKey = row.key();
                    final DataFrame<R,C> group = getGroup(groupKey);
                    group.cols().forEach(column -> {
                        final C colKey = column.key();
                        if (result.cols().contains(colKey)) {
                            final double value = column.compute(stat, 0, column.size());
                            row.setDouble(colKey, value);
                        }
                    });
                });
                return result;
            } catch (Exception ex) {
                throw new DataFrameException("Failed to compute grouped row stats: " + ex.getMessage(), ex);
            }
        }
    }

}
