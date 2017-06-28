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

import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameOptions;
import com.zavtech.morpheus.stats.Statistic1;

/**
 * The reference implementation of Stats to provide rolling window statistics in either the row or column dimension of a DataFrame
 *
 * @param <R>       the row key type
 * @param <C>       the column key type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class XDataFrameStatsRolling<R,C> extends XDataFrameStatsBase<R,C> {

    private int axis;
    private int windowSize;
    private XDataFrame<R,C> frame;

    /**
     * Constructor
     * @param frame         the frame to operate on
     * @param windowSize    the window size for rolling stats
     * @param parallel      true if should operate in parallel mode
     * @param columns       true to operate in column dimension, false for row dimension
     */
    XDataFrameStatsRolling(XDataFrame<R,C> frame, int windowSize, boolean parallel, boolean columns) {
        super(columns, parallel);
        this.frame = frame;
        this.axis = columns ? 1 : 0;
        this.windowSize = windowSize;
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
        switch (axis) {
            case 0: return frame.cols().count() > windowSize;
            case 1: return frame.rows().count() >= windowSize;
            default:    throw new IllegalStateException("Unsupported axis code: " + axis);
        }
    }


    @Override
    protected XDataFrame<R,C> createResult(Statistic1 statistic, boolean viable) {
        return (XDataFrame<R,C>)frame.copy().applyDoubles(v -> Double.NaN);
    }


    @Override
    protected StatisticAction createStatisticAction(Statistic1 statistic, XDataFrame<R,C> result) {
        switch (axis) {
            case 0: return new RowRollingStatistics(0, rowCount(), statistic, result);
            case 1: return new ColumnRollingStatistics(0, colCount(), statistic, result);
            default:    throw new DataFrameException("Unsupported axis code: " + axis);
        }
    }


    /**
     * Action to compute rolling window statistic on the rows of a DataFrame
     */
    private class RowRollingStatistics extends StatisticAction {

        private int to;
        private int from;
        private Statistic1 statistic;
        private XDataFrame<R,C> result;

        /**
         * Constructor
         * @param from          the from row index
         * @param to            the to row index
         * @param statistic     the uni-variate statistic
         * @param result        the target frame to write results to
         */
        RowRollingStatistics(int from, int to, Statistic1 statistic, XDataFrame<R, C> result) {
            this.from = from;
            this.to = to;
            this.result = result;
            this.statistic = statistic.copy();
        }

        @Override
        public void compute() {
            final int count = to - from + 1;
            final int threshold = isParallel() ? DataFrameOptions.getRowSplitThreshold(frame) : Integer.MAX_VALUE;
            if (count <= threshold) {
                final int rowCount = frame.rows().count();
                final int colCount = frame.cols().count();
                for (int rowIndex = from; rowIndex < rowCount; ++rowIndex) {
                    for (int colIndex = windowSize-1; colIndex < colCount; colIndex++) {
                        this.statistic.reset();
                        final int from = colIndex - windowSize + 1;
                        for (int i = from; i <= colIndex; ++i) {
                            final double value = frame.data().getDouble(rowIndex, i);
                            this.statistic.add(value);
                        }
                        final double statValue = statistic.getValue();
                        this.result.data().setDouble(rowIndex, colIndex, statValue);
                    }
                }
            } else {
                final int splitCount = (to - from) / 2;
                final int midPoint = from + splitCount;
                invokeAll(
                    new RowRollingStatistics(from, midPoint, statistic, result),
                    new RowRollingStatistics(midPoint+1, to, statistic, result)
                );
            }
        }

    }

    /**
     * Action to compute rolling window statistic on the columns of a DataFrame
     */
    private class ColumnRollingStatistics extends StatisticAction {

        private int to;
        private int from;
        private Statistic1 statistic;
        private XDataFrame<R,C> result;

        /**
         * Constructor
         * @param from          the from column index
         * @param to            the to column index
         * @param statistic     the uni-variate statistic
         * @param result        the target frame to write results to
         */
        ColumnRollingStatistics(int from, int to, Statistic1 statistic, XDataFrame<R, C> result) {
            this.from = from;
            this.to = to;
            this.result = result;
            this.statistic = statistic.copy();
        }

        @Override
        public void compute() {
            final int count = to - from + 1;
            final int threshold = isParallel() ? DataFrameOptions.getColumnSplitThreshold(frame) : Integer.MAX_VALUE;
            if (count <= threshold) {
                final int rowCount = frame.rows().count();
                final int colCount = frame.cols().count();
                for (int colIndex = from; colIndex < colCount; ++colIndex) {
                    for (int rowIndex = windowSize-1; rowIndex < rowCount; rowIndex++) {
                        this.statistic.reset();
                        final int from = rowIndex - windowSize + 1;
                        for (int i = from; i <= rowIndex; ++i) {
                            final double value = frame.data().getDouble(i, colIndex);
                            this.statistic.add(value);
                        }
                        final double statValue = statistic.getValue();
                        this.result.data().setDouble(rowIndex, colIndex, statValue);
                    }
                }
            } else {
                final int splitCount = (to - from) / 2;
                final int midPoint = from + splitCount;
                invokeAll(
                    new ColumnRollingStatistics(from, midPoint, statistic, result),
                    new ColumnRollingStatistics(midPoint+1, to, statistic, result)
                );
            }
        }
    }
}
