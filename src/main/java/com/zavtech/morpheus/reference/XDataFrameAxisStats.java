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

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameAxisStats;
import com.zavtech.morpheus.frame.DataFrameColumn;
import com.zavtech.morpheus.frame.DataFrameCursor;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameOptions;
import com.zavtech.morpheus.frame.DataFrameRow;
import com.zavtech.morpheus.frame.DataFrameVector;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.stats.Correlation;
import com.zavtech.morpheus.stats.Covariance;
import com.zavtech.morpheus.stats.StatType;
import com.zavtech.morpheus.stats.Statistic1;
import com.zavtech.morpheus.stats.Statistic2;
import com.zavtech.morpheus.stats.Stats;

/**
 * The reference implementation of the DataFrameAxisStats interface as applied to the column dimension of a DataFrame
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
class XDataFrameAxisStats<K,R,C,X,Y> extends XDataFrameStatsBase<X,Y> implements DataFrameAxisStats<K,R,C,X,Y> {

    private int axis;
    private boolean parallel;
    private XDataFrame<R,C> frame;

    /**
     * Constructor
     * @param frame     the frame to operate on
     * @param parallel  true to operate in parallel mode
     */
    XDataFrameAxisStats(XDataFrame<R,C> frame, boolean parallel, boolean columns) {
        super(columns, parallel);
        this.frame = frame;
        this.parallel = parallel;
        this.axis = columns ? 1 : 0;
    }

    /**
     * Returns true if this represents the row dimension
     * @return  true if represents the row dimension
     */
    protected boolean isRow() {
        return axis == 0;
    }


    @Override
    protected final int rowCount() {
        return frame.rows().count();
    }


    @Override
    protected final int colCount() {
        return frame.cols().count();
    }


    @Override
    protected boolean isViable(Statistic1 statistic) {
        return true;
    }


    @Override
    @SuppressWarnings("unchecked")
    protected StatisticAction createStatisticAction(Statistic1 statistic, XDataFrame<X,Y> result) {
        if (isRow()) {
            final int fromOrdinal = 0;
            final int toOrdinal = result.rowCount()-1;
            return new RowStatistics(fromOrdinal, toOrdinal, (XDataFrame<R,StatType>)result, statistic);
        } else {
            final int fromOrdinal = 0;
            final int toOrdinal = result.rowCount()-1;
            return new ColumnStatistics(fromOrdinal, toOrdinal, (XDataFrame<C,StatType>)result, statistic);
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    protected XDataFrame<X,Y> createResult(Statistic1 statistic, boolean viable) {
        final StatType stat = statistic.getType();
        if (isRow()) {
            final Index<X> rowKeys = (Index<X>)Index.of(frame.rows().filter(DataFrameVector::isNumeric).keyArray());
            final Index<Y> colKeys = (Index<Y>)Index.singleton(stat);
            return (XDataFrame<X,Y>)DataFrame.ofDoubles(rowKeys, colKeys);
        } else {
            final Index<X> colKeys = (Index<X>)Index.singleton(stat);
            final Index<Y> rowKeys = (Index<Y>)Index.of(frame.cols().filter(DataFrameVector::isNumeric).keyArray());
            return (XDataFrame<X,Y>)DataFrame.ofDoubles(rowKeys, colKeys);
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public DataFrame<K,K> covariance() {
        try {
            final Statistic2 covariance = new Covariance();
            if (isRow()) {
                final Index<K> rowKeys = (Index<K>)Index.of(frame.rows().filter(DataFrameVector::isNumeric).keyArray());
                final XDataFrame<K,K> result = (XDataFrame<K,K>)DataFrame.ofDoubles(rowKeys, rowKeys);
                final StatisticAction action = new BivariateRowStatistics(0, result.rows().count()-1, result, covariance);
                if (isParallel()) ForkJoinPool.commonPool().invoke(action); else action.compute();
                return result;
            } else {
                final Index<K> colKeys = (Index<K>)Index.of(frame.cols().filter(DataFrameVector::isNumeric).keyArray());
                final XDataFrame<K,K> result = (XDataFrame<K,K>)DataFrame.ofDoubles(colKeys, colKeys);
                final StatisticAction action = new BivariateColumnStatistics(0, result.cols().count()-1, result, covariance);
                if (isParallel()) ForkJoinPool.commonPool().invoke(action); else action.compute();
                return result;
            }
        } catch (Exception ex) {
            throw new DataFrameException("Failed to compute covariance matrix for DataFrame", ex);
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public DataFrame<K,K> correlation() {
        try {
            final Statistic2 correlation = new Correlation();
            if (isRow()) {
                final Index<K> rowKeys = (Index<K>)Index.of(frame.rows().filter(DataFrameVector::isNumeric).keyArray());
                final XDataFrame<K,K> result = (XDataFrame<K,K>)DataFrame.ofDoubles(rowKeys, rowKeys);
                final StatisticAction action = new BivariateRowStatistics(0, result.rowCount()-1, result, correlation);
                if (isParallel()) ForkJoinPool.commonPool().invoke(action); else action.compute();
                return result;
            } else {
                final Index<K> colKeys = (Index<K>)Index.of(frame.cols().filter(DataFrameVector::isNumeric).keyArray());
                final XDataFrame<K,K> result = (XDataFrame<K,K>)DataFrame.ofDoubles(colKeys, colKeys);
                final StatisticAction action = new BivariateColumnStatistics(0, result.colCount()-1, result, correlation);
                if (isParallel()) ForkJoinPool.commonPool().invoke(action); else action.compute();
                return result;
            }
        } catch (Exception ex) {
            throw new DataFrameException("Failed to compute correlation matrix for DataFrame", ex);
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public double covariance(K key1, K key2) {
        final int rowCount = frame.rows().count();
        final int colCount = frame.cols().count();
        if (rowCount == 0 || colCount == 0) {
            return Double.NaN;
        } else if (isRow()) {
            final DataFrameRow<R,C> row1 = frame.rowAt((R) key1);
            final DataFrameRow<R,C> row2 = frame.rowAt((R) key2);
            final Statistic2 correlation = new Covariance();
            for (int i = 0; i < colCount; ++i) {
                final double v1 = row1.getDouble(i);
                final double v2 = row2.getDouble(i);
                correlation.add(v1, v2);
            }
            return correlation.getValue();
        } else {
            final DataFrameColumn<R,C> column1 = frame.colAt((C) key1);
            final DataFrameColumn<R,C> column2 = frame.colAt((C) key2);
            final Statistic2 correlation = new Covariance();
            for (int i = 0; i < rowCount; ++i) {
                final double v1 = column1.getDouble(i);
                final double v2 = column2.getDouble(i);
                correlation.add(v1, v2);
            }
            return correlation.getValue();
        }
    }


    @Override
    @SuppressWarnings("unchecked")
    public double correlation(K key1, K key2) {
        final int rowCount = frame.rows().count();
        final int colCount = frame.cols().count();
        if (rowCount == 0 || colCount == 0) {
            return Double.NaN;
        } else if (isRow()) {
            final DataFrameRow<R,C> row1 = frame.rowAt((R) key1);
            final DataFrameRow<R,C> row2 = frame.rowAt((R) key2);
            final Statistic2 correlation = new Correlation();
            for (int i = 0; i < colCount; ++i) {
                final double v1 = row1.getDouble(i);
                final double v2 = row2.getDouble(i);
                correlation.add(v1, v2);
            }
            return correlation.getValue();
        } else {
            final DataFrameColumn<R,C> column1 = frame.colAt((C) key1);
            final DataFrameColumn<R,C> column2 = frame.colAt((C) key2);
            final Statistic2 correlation = new Correlation();
            for (int i = 0; i < rowCount; ++i) {
                final double v1 = column1.getDouble(i);
                final double v2 = column2.getDouble(i);
                correlation.add(v1, v2);
            }
            return correlation.getValue();
        }
    }


    @Override
    public DataFrame<R,C> ewma(int halfLife) {
        final XDataFrame<R,C> result = (XDataFrame<R,C>)frame.copy();
        if (parallel) {
            final int colCount = result.cols().count();
            ForkJoinPool.commonPool().invoke(new Ewma(result, 0, colCount-1, 2, halfLife));
        } else {
            final int colCount = result.cols().count();
            new Ewma(result, 0, colCount-1, Integer.MAX_VALUE, halfLife).compute();
        }
        return result;
    }


    @Override
    public DataFrame<R,C> ewmstd(int halfLife) {
        return null;
    }


    @Override
    public DataFrame<R,C> ewmvar(int halfLife) {
        return null;
    }

    @Override
    public Stats<DataFrame<R,C>> rolling(int windowSize) {
        return new XDataFrameStatsRolling<>(frame, windowSize, parallel, !isRow());
    }


    @Override
    public Stats<DataFrame<R,C>> expanding(int minPeriods) {
        return new XDataFrameStatsExpanding<>(frame, minPeriods, parallel, !isRow());
    }

    /**
     * A RecursiveAction used to compute stats over the rows of a DataFrame
     */
    private class RowStatistics extends StatisticAction {

        private int from;
        private int to;
        private Statistic1 statistic;
        private XDataFrame<R,StatType> target;

        /**
         * Constructor
         * @param from          the from column index
         * @param to            the to column index
         * @param target        the target DataFrame
         * @param statistic     the statistic instance
         */
        RowStatistics(int from, int to, XDataFrame<R,StatType> target, Statistic1 statistic) {
            this.from = from;
            this.to = to;
            this.target = target;
            this.statistic = statistic;
        }

        @Override
        public void compute() {
            final int count = to - from + 1;
            final int threshold = isParallel() ? DataFrameOptions.getRowSplitThreshold(frame) : Integer.MAX_VALUE;
            if (count <= threshold) {
                final int length = frame.cols().count();
                final XDataFrameRow<R,C> row = new XDataFrameRow<>(frame, false);
                for (int rowOrdinal = from; rowOrdinal <= to; ++rowOrdinal) {
                    final R rowKey = target.rows().key(rowOrdinal);
                    final double statValue = row.moveTo(rowKey).compute(statistic, 0, length);
                    this.target.data().setDouble(rowOrdinal, 0, statValue);
                }
            } else {
                final int splitCount = (to - from) / 2;
                final int midPoint = from + splitCount;
                invokeAll(
                    new RowStatistics(from, midPoint, target, statistic.copy().reset()),
                    new RowStatistics(midPoint+1, to, target, statistic.copy().reset())
                );
            }
        }
    }


    /**
     * A RecursiveAction used to compute stats over the columns of a DataFrame
     */
    private class ColumnStatistics extends StatisticAction {

        private int from;
        private int to;
        private Statistic1 statistic;
        private XDataFrame<C,StatType> target;

        /**
         * Constructor
         * @param from          the from column index
         * @param to            the to column index
         * @param target        the target DataFrame
         * @param statistic     the statistic instance
         */
        ColumnStatistics(int from, int to, XDataFrame<C,StatType> target, Statistic1 statistic) {
            this.from = from;
            this.to = to;
            this.target = target;
            this.statistic = statistic.copy();
        }

        @Override
        public void compute() {
            final int count = to - from + 1;
            final int threshold = isParallel() ? DataFrameOptions.getColumnSplitThreshold(frame) : Integer.MAX_VALUE;
            if (count <= threshold) {
                final int length = frame.rowCount();
                final XDataFrameColumn<R,C> column = new XDataFrameColumn<>(frame, false);
                for (int colOrdinal = from; colOrdinal <= to; ++colOrdinal) {
                    final C colKey = target.rows().key(colOrdinal);
                    final double statValue = column.moveTo(colKey).compute(statistic, 0, length);
                    this.target.data().setDouble(colOrdinal, 0, statValue);
                }
            } else {
                final int splitCount = (to - from) / 2;
                final int midPoint = from + splitCount;
                invokeAll(
                    new ColumnStatistics(from, midPoint, target, statistic),
                    new ColumnStatistics(midPoint+1, to, target, statistic)
                );
            }
        }
    }



    /**
     * RecursiveAction used to compute EWMA for a columns in the DataFrame
     */
    private class Ewma extends RecursiveAction {

        private int from;
        private int to;
        private int threshold;
        private double halfLife;
        private XDataFrame<R,C> result;

        /**
         * Constructor
         * @param result    the frame to apply results to
         * @param from      the from index
         * @param to        the to index
         * @param threshold the threshold for split
         * @param halfLife  the half-life for EWMA
         */
        Ewma(XDataFrame<R,C> result, int from, int to, int threshold, double halfLife) {
            this.result = result;
            this.from = from;
            this.to = to;
            this.threshold = threshold;
            this.halfLife = halfLife;
        }

        @Override
        protected void compute() {
            final int remainder = to - from + 1;
            if (remainder <= threshold) {
                final int rowCount = result.rows().count();
                final double weight = Math.log(0.5d) / halfLife;
                final double alpha = 1d - Math.exp(weight);
                for (int colOrdinal=from; colOrdinal<=to; ++colOrdinal) {
                    final DataFrameCursor<R,C> source = frame.cursor().moveTo(0, colOrdinal);
                    final DataFrameCursor<R,C> target = result.cursor().moveTo(0, colOrdinal);
                    if (rowCount > 0) target.setDouble(source.getDouble());
                    for (int rowOrdinal=1; rowOrdinal<rowCount; ++rowOrdinal) {
                        final double rawValue = source.moveToRow(rowOrdinal).getDouble();
                        final double emaPrior = target.moveToRow(rowOrdinal-1).getDouble();
                        final double emaValue = rawValue * alpha + (1d - alpha) * emaPrior;
                        target.moveToRow(rowOrdinal).setDouble(emaValue);
                    }
                }
            } else {
                final int splitCount = (to - from) / 2;
                final int midPoint = from + splitCount;
                invokeAll(
                    new Ewma(result, from, midPoint, threshold, halfLife),
                    new Ewma(result, midPoint+1, to, threshold, halfLife)
                );
            }
        }
    }


    /**
     * A Callable to compute a bi-variate statistic based on row vectors of a DataFrame
     */
    private class BivariateRowStatistics extends StatisticAction {

        private int from;
        private int to;
        private XDataFrame<K,K> result;
        private Statistic2 statistic;

        /**
         * Constructor
         * @param from      the from column index
         * @param to        the to column index
         * @param result    the result frame to write to
         * @param statistic the statistic
         */
        private BivariateRowStatistics(int from, int to, XDataFrame<K,K> result, Statistic2 statistic) {
            this.from = from;
            this.to = to;
            this.result = result;
            this.statistic = statistic.copy();
        }

        @Override
        @SuppressWarnings("unchecked")
        public void compute() {
            final int count = to - from + 1;
            final int threshold = isParallel() ? DataFrameOptions.getRowSplitThreshold(frame) : Integer.MAX_VALUE;
            if (count <= threshold) {
                final int rowCount = result.rowCount();
                final int colCount = frame.colCount();
                final XDataFrameRow<R,C> row1 = new XDataFrameRow<>(frame, false);
                final XDataFrameRow<R,C> row2 = new XDataFrameRow<>(frame, false);
                for (int rowIndex1 = from; rowIndex1 <= to; ++rowIndex1) {
                    row1.moveTo((R)result.rows().key(rowIndex1));
                    for (int rowIndex2 = 0; rowIndex2 < rowCount; ++rowIndex2) {
                        row2.moveTo((R)result.rows().key(rowIndex2));
                        if (row1.isNumeric() && row2.isNumeric()) {
                            this.statistic.reset();
                            for (int colIndex = 0; colIndex < colCount; ++colIndex) {
                                final double v1 = row1.getDouble(colIndex);
                                final double v2 = row2.getDouble(colIndex);
                                this.statistic.add(v1, v2);
                            }
                            final double statValue = statistic.getValue();
                            this.result.data().setDouble(rowIndex1, rowIndex2, statValue);
                        }
                    }
                }
            } else {
                final int splitCount = (to - from) / 2;
                final int midPoint = from + splitCount;
                invokeAll(
                    new BivariateRowStatistics(from, midPoint, result, statistic),
                    new BivariateRowStatistics(midPoint+1, to, result, statistic)
                );
            }
        }
    }

    /**
     * A Callable to compute a bi-variate statistic based on column vectors of a DataFrame
     */
    private class BivariateColumnStatistics extends StatisticAction {

        private int from;
        private int to;
        private XDataFrame<K,K> result;
        private Statistic2 statistic;

        /**
         * Constructor
         * @param from      the from column index
         * @param to        the to column index
         * @param result    the result frame to write to
         * @param statistic the statistic
         */
        private BivariateColumnStatistics(int from, int to, XDataFrame<K, K> result, Statistic2 statistic) {
            this.from = from;
            this.to = to;
            this.result = result;
            this.statistic = statistic.copy();
        }

        @Override
        @SuppressWarnings("unchecked")
        public void compute() {
            final int count = to - from + 1;
            final int threshold = isParallel() ? DataFrameOptions.getColumnSplitThreshold(frame) : Integer.MAX_VALUE;
            if (count <= threshold) {
                final int rowCount = frame.rowCount();
                final int colCount = result.colCount();
                final XDataFrameColumn<R,C> column1 = new XDataFrameColumn<>(frame, false);
                final XDataFrameColumn<R,C> column2 = new XDataFrameColumn<>(frame, false);
                for (int colIndex1 = from; colIndex1 <= to; ++colIndex1) {
                    column1.moveTo((C)result.cols().key(colIndex1));
                    for (int colIndex2 = 0; colIndex2 < colCount; ++ colIndex2) {
                        column2.moveTo((C)result.cols().key(colIndex2));
                        if (column1.isNumeric() && column2.isNumeric()) {
                            this.statistic.reset();
                            for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
                                final double v1 = column1.getDouble(rowIndex);
                                final double v2 = column2.getDouble(rowIndex);
                                this.statistic.add(v1, v2);
                            }
                            final double statValue = statistic.getValue();
                            this.result.data().setDouble(colIndex1, colIndex2, statValue);
                        }
                    }
                }
            } else {
                final int splitCount = (to - from) / 2;
                final int midPoint = from + splitCount;
                invokeAll(
                    new BivariateColumnStatistics(from, midPoint, result, statistic),
                    new BivariateColumnStatistics(midPoint+1, to, result, statistic)
                );
            }
        }
    }

}
