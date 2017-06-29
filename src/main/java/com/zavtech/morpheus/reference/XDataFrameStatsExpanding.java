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
 * The reference implementation of Stats to provide expanding window statistics in either the row or column dimension of a DataFrame
 *
 * @param <R>       the row key type
 * @param <C>       the column key type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class XDataFrameStatsExpanding<R,C> extends XDataFrameStatsBase<R,C> {

    private int axis;
    private int minPeriods;
    private XDataFrame<R,C> frame;

    /**
     * Constructor
     * @param frame     the frame to operate om
     */
    XDataFrameStatsExpanding(XDataFrame<R,C> frame, int minPeriods, boolean parallel, boolean columns) {
        super(columns, parallel);
        this.frame = frame;
        this.minPeriods = minPeriods;
        this.axis = columns ? 1 : 0;
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
            case 0: return frame.cols().count() > 0;
            case 1: return frame.rows().count() > 0;
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
            case 0:     return new RowExpandingStatistics(0, rowCount()-1, statistic, result);
            case 1:     return new ColumnExpandingStatistics(0, colCount()-1, statistic, result);
            default:    throw new DataFrameException("Unsupported axis code: " + axis);
        }
    }


    /**
     * Action to compute expanding window statistic on the rows of a DataFrame
     */
    private class RowExpandingStatistics extends StatisticAction {

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
        RowExpandingStatistics(int from, int to, Statistic1 statistic, XDataFrame<R, C> result) {
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
                final int colCount = frame.cols().count();
                for (int rowIndex = from; rowIndex <= to; ++rowIndex) {
                    this.statistic.reset();
                    for (int colIndex = 0; colIndex < colCount; colIndex++) {
                        this.statistic.add(frame.data().getDouble(rowIndex, colIndex));
                        if (statistic.getN() < minPeriods) {
                            this.result.data().setDouble(rowIndex, colIndex, Double.NaN);
                        } else {
                            final double statValue = statistic.getValue();
                            this.result.data().setDouble(rowIndex, colIndex, statValue);
                        }
                    }
                }
            } else {
                final int splitCount = (to - from) / 2;
                final int midPoint = from + splitCount;
                invokeAll(
                    new RowExpandingStatistics(from, midPoint, statistic, result),
                    new RowExpandingStatistics(midPoint+1, to, statistic, result)
                );
            }
        }

    }

    /**
     * Action to compute expanding window statistic on the columns of a DataFrame
     */
    private class ColumnExpandingStatistics extends StatisticAction {

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
        ColumnExpandingStatistics(int from, int to, Statistic1 statistic, XDataFrame<R, C> result) {
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
                for (int colIndex=from; colIndex <= to; ++colIndex) {
                    this.statistic.reset();
                    for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
                        this.statistic.add(frame.data().getDouble(rowIndex, colIndex));
                        if (statistic.getN() < minPeriods) {
                            this.result.data().setDouble(rowIndex, colIndex, Double.NaN);
                        } else {
                            final double statValue = statistic.getValue();
                            this.result.data().setDouble(rowIndex, colIndex, statValue);
                        }
                    }
                }
            } else {
                final int splitCount = (to - from) / 2;
                final int midPoint = from + splitCount;
                invokeAll(
                    new ColumnExpandingStatistics(from, midPoint, statistic, result),
                    new ColumnExpandingStatistics(midPoint + 1, to, statistic, result)
                );
            }
        }
    }

}
