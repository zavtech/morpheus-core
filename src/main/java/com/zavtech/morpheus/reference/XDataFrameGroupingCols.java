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
import com.zavtech.morpheus.frame.DataFrameColumn;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameGrouping;
import com.zavtech.morpheus.frame.DataFrameOptions;
import com.zavtech.morpheus.frame.DataFrameVector;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.stats.Statistic1;
import com.zavtech.morpheus.stats.Stats;
import com.zavtech.morpheus.stats.StatsAssembler;
import com.zavtech.morpheus.util.Tuple;

/**
 * An implementation of the DataFrameGrouping interface that groups columns of a DataFrame
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class XDataFrameGroupingCols<R,C> implements DataFrameGrouping.Cols<R,C> {

    private int depth;
    private XDataFrame<R,C> source;
    private Map<Tuple,Array<C>> groupKeysMap;

    /**
     * Constructor
     * @param source        the source frame for groups
     * @param depth         the grouping depth
     * @param groupKeysMap  the apply of keys for each group
     */
    private XDataFrameGroupingCols(XDataFrame<R,C> source, int depth, Map<Tuple,Array<C>> groupKeysMap) {
        this.source = source;
        this.depth = depth;
        this.groupKeysMap = groupKeysMap;
    }

    /**
     * Returns a new grouping engine by columns
     * @param source    the source frame to group
     * @param parallel  true for parallel grouping
     * @param rowKeys   the row keys to group by
     * @param <R>       the row key type
     * @param <C>       the column key type
     * @return          the newly create grouping engine
     */
    static <R,C> XDataFrameGroupingCols<R,C> of(XDataFrame<R,C> source, boolean parallel, Array<R> rowKeys) {
        final int[] ordinals = source.rowKeys().ordinals(rowKeys).toArray();
        return XDataFrameGroupingCols.of(source, parallel, column -> {
            final Object[] values = new Object[ordinals.length];
            for (int i=0; i<ordinals.length; ++i) {
                values[i] = column.getValue(ordinals[i]);
            }
            return Tuple.of(values);
        });
    }


    /**
     * Returns a new grouping engine by columns
     * @param source    the source frame to group
     * @param function  the grouping function
     * @param <R>       the row key
     * @param <C>       the column key
     * @return          the newly create grouping engine
     */
    static <R,C> XDataFrameGroupingCols<R,C> of(XDataFrame<R,C> source, boolean parallel, Function<DataFrameColumn<R,C>,Tuple> function) {
        final int depth = source.cols().first().map(function).map(Tuple::size).orElse(0);
        final GroupColumnsTask<R,C> task = new GroupColumnsTask<>(source, 0, source.colCount()-1, depth, parallel, function);
        return parallel ? ForkJoinPool.commonPool().invoke(task) : task.compute();
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
    public final Stats<DataFrame<R,Tuple>> stats(int level) {
        return new GroupedColStats(level);
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
        final Array<C> groupKeys = groupKeysMap.get(groupKey);
        if (groupKeys == null) {
            throw new DataFrameException("No DataFrame for group " + groupKey);
        } else {
            final Index<R> rowKeys = source.rowKeys().readOnly();
            final Index<C> colKeys = source.colKeys().filter(groupKeys);
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
     * Combines all contents from the argument grouping with this grouping
     * This method is used as part of the Fork & Join parallel grouping functionality
     * @param other     the other grouping to combine results
     * @return          this grouping with argument contents included
     */
    private XDataFrameGroupingCols<R,C> combine(XDataFrameGroupingCols<R,C> other) {
        other.groupKeysMap.forEach((groupKey1, groupKeys1) -> {
            final Array<C> groupKeys2 = this.groupKeysMap.get(groupKey1);
            if (groupKeys2 == null) {
                this.groupKeysMap.put(groupKey1, groupKeys1);
            } else {
                final Class<C> type = groupKeys1.type();
                final int length = groupKeys2.length();
                final int newLength = length + groupKeys1.length();
                final Array<C> combinedKeys = Array.of(type, newLength);
                combinedKeys.update(0, groupKeys2, 0, groupKeys2.length());
                combinedKeys.update(length, groupKeys1, 0, groupKeys1.length());
                this.groupKeysMap.put(groupKey1, combinedKeys);
            }
        });
        return this;
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
     * A RecursiveTask implementation to group a DataFrame along the column dimension
     */
    private static class GroupColumnsTask<X,Y> extends RecursiveTask<XDataFrameGroupingCols<X,Y>> {

        private int from;
        private int to;
        private int depth;
        private boolean parallel;
        private XDataFrame<X,Y> source;
        private int threshold = Integer.MAX_VALUE;
        private Function<DataFrameColumn<X,Y>,Tuple> function;

        /**
         * Constructor
         * @param source    the source frame to group
         * @param from      the from index (inclusive)
         * @param to        the to index (inclusive)
         * @param depth     the grouping depth
         * @param parallel  true for parallel mode
         * @param function  the row grouping function
         */
        private GroupColumnsTask(XDataFrame<X,Y> source, int from, int to, int depth, boolean parallel, Function<DataFrameColumn<X,Y>,Tuple> function) {
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
        protected XDataFrameGroupingCols<X,Y> compute() {
            final int count = to - from + 1;
            if (count > threshold) {
                return split();
            } else {
                final Class<Y> keyType = source.cols().keyType();
                final Map<Tuple,ArrayBuilder<Y>> groupKeyBuilderMap = new HashMap<>();
                final XDataFrameColumn<X,Y> column = new XDataFrameColumn<>(source, false);
                for (int i=from; i<=to; ++i) {
                    column.moveTo(i);
                    final Y key = column.key();
                    try {
                        final Tuple groupKey = function.apply(column);
                        for (int level=0; level<depth; ++level) {
                            final Tuple tuple = groupKey.filter(0, level + 1);
                            ArrayBuilder<Y> groupKeyBuilder = groupKeyBuilderMap.get(tuple);
                            if (groupKeyBuilder == null) {
                                groupKeyBuilder = ArrayBuilder.of(1000, keyType);
                                groupKeyBuilderMap.put(tuple, groupKeyBuilder);
                            }
                            groupKeyBuilder.add(key);
                        }
                    } catch (Exception ex) {
                        throw new DataFrameException("Grouping failed at column: " + key, ex);
                    }
                }
                final Map<Tuple,Array<Y>> groupKeyMap = crystallize(groupKeyBuilderMap);
                return new XDataFrameGroupingCols<>(source, depth, groupKeyMap);
            }
        }

        /**
         * Splits into two grouping operations and then combines the results of each
         * @return      the combined grouping of the two split grouping tasks
         */
        private XDataFrameGroupingCols<X,Y> split() {
            final int splitCount = (to - from) / 2;
            final int midPoint = from + splitCount;
            final GroupColumnsTask<X,Y> left  = new GroupColumnsTask<>(source, from, midPoint, depth, parallel, function);
            final GroupColumnsTask<X,Y> right = new GroupColumnsTask<>(source, midPoint + 1, to, depth, parallel, function);
            left.fork();
            final XDataFrameGroupingCols<X,Y> rightAns = right.compute();
            final XDataFrameGroupingCols<X,Y> leftAns  = left.join();
            return rightAns.combine(leftAns);
        }
    }


    /**
     * Computes bulk statistics over grouped rows
     */
    private class GroupedColStats extends StatsAssembler<DataFrame<R,Tuple>> {


        private int level;

        /**
         * Constructor
         * @param level the group level
         */
        GroupedColStats(int level) {
            this.level = level;
        }

        @Override
        protected DataFrame<R,Tuple> compute(Statistic1 stat) {
            try {
                final int groupCount = getGroupCount(level);
                final Array<Tuple> groupKeys = getGroupKeys(level).collect(ArrayCollector.of(Tuple.class, groupCount));
                final Array<R> rowKeys = source.rows().filter(DataFrameVector::isNumeric).keyArray();
                final DataFrame<R,Tuple> result = DataFrame.ofDoubles(rowKeys, groupKeys);
                result.cols().forEach(column -> {
                    final Tuple groupKey = column.key();
                    final DataFrame<R,C> group = getGroup(groupKey);
                    group.rows().forEach(row -> {
                        final R rowKey = row.key();
                        if (result.rows().contains(rowKey)) {
                            final double value = row.compute(stat, 0, row.size());
                            column.setDouble(rowKey, value);
                        }
                    });
                });
                return result;
            } catch (Exception ex) {
                throw new DataFrameException("Failed to compute grouped column stats: " + ex.getMessage(), ex);
            }
        }
    }


}
