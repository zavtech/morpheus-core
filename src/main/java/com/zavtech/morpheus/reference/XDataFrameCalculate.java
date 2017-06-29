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

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameCalculate;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.stats.Statistic1;
import com.zavtech.morpheus.stats.StdDev;

/**
 * The reference implementation of the DataFrameCalculate interface
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class XDataFrameCalculate<R,C> implements DataFrameCalculate<R,C> {

    private XDataFrame<R,C> frame;

    /**
     * Constructor
     * @param frame the frame to operate on
     */
    XDataFrameCalculate(XDataFrame<R,C> frame) {
        this.frame = frame;
    }


    @Override
    public DataFrame<R, C> logReturns() {
        final int rowCount = frame.rows().count();
        final int colCount = frame.cols().count();
        final DataFrame<R,C> result = frame.copy().applyDoubles(v -> Double.NaN);
        for (int i=1; i<rowCount; ++i) {
            for (int j=0; j<colCount; ++j) {
                final double v0 = frame.data().getDouble(i - 1, j);
                final double v1 = frame.data().getDouble(i, j);
                final double logReturn = Math.log(v1 / v0);
                result.data().setDouble(i, j, logReturn);
            }
        }
        return result;
    }


    @Override
    public DataFrame<R, C> cumReturns() {
        final int rowCount = frame.rows().count();
        final int colCount = frame.cols().count();
        final DataFrame<R,C> result = frame.copy().applyDoubles(v -> Double.NaN);
        for (int i=1; i<rowCount; ++i) {
            for (int j=0; j<colCount; ++j) {
                final double v0 = frame.data().getDouble(0, j);
                final double v1 = frame.data().getDouble(i, j);
                final double cumReturn = v1 / v0 - 1d;
                result.data().setDouble(i, j, cumReturn);
            }
        }
        return result;
    }


    @Override
    public DataFrame<R, C> percentChanges() {
        final int rowCount = frame.rows().count();
        final int colCount = frame.cols().count();
        final DataFrame<R,C> result = frame.copy().applyDoubles(v -> Double.NaN);
        for (int i=1; i<rowCount; ++i) {
            for (int j=0; j<colCount; ++j) {
                final double v0 = frame.data().getDouble(i - 1, j);
                final double v1 = frame.data().getDouble(i, j);
                final double percentChange = v1 / v0 - 1d;
                result.data().setDouble(i, j, percentChange);
            }
        }
        return result;
    }


    @Override
    public DataFrame<R,C> sma(int windowSize) {
        final int rowCount = frame.rows().count();
        final Array<C> colKeys = frame.cols().keyArray();
        final Index<C> colIndex = Index.of(colKeys);
        if (rowCount < windowSize) {
            return DataFrame.ofDoubles(Index.empty(), colIndex);
        } else {
            final int colCount = frame.cols().count();
            final Array<R> rowKeys = frame.rowKeys().toArray(windowSize-1, rowCount);
            final Index<R> rowIndex = Index.of(rowKeys);
            final DataFrame<R,C> result = DataFrame.ofDoubles(rowIndex, colIndex);
            for (int rowOrdinal=windowSize-1; rowOrdinal<rowCount; ++rowOrdinal) {
                for (int colOrdinal=0; colOrdinal<colCount; ++colOrdinal) {
                    double sum = 0d;
                    double count = 0;
                    final int from = rowOrdinal-windowSize+1;
                    for (int i=from; i<=rowOrdinal; ++i) {
                        count++;
                        sum += frame.data().getDouble(i, colOrdinal);
                    }
                    result.data().setDouble(from, colOrdinal, sum / count);
                }
            }
            return result;
        }
    }


    @Override
    public DataFrame<R,C> ema(int windowSize) {
        final int rowCount = frame.rows().count();
        final DataFrame<R,C> result = frame.copy().applyDoubles(v -> Double.NaN);
        if (rowCount > 0) {
            final int colCount = frame.cols().count();
            for (int colIndex=0; colIndex<colCount; ++colIndex) {
                final double value = frame.data().getDouble(0, colIndex);
                result.data().setDouble(0, colIndex, value);
            }
            final double alpha = 2 / (windowSize + 1d);
            for (int rowIndex=1; rowIndex<rowCount; ++rowIndex) {
                for (int j=0; j<colCount; ++j) {
                    final double rawValue = frame.data().getDouble(rowIndex, j);
                    final double emaPrior = result.data().getDouble(rowIndex - 1, j);
                    final double emaValue = rawValue * alpha + (1d - alpha) * emaPrior;
                    result.data().setDouble(rowIndex, j, emaValue);
                }
            }
        }
        return result;
    }


    @Override
    public DataFrame<R,C> stdDev(int windowSize) {
        final int rowCount = frame.rows().count();
        final Array<C> colKeys = frame.cols().keyArray();
        final Index<C> colIndex = Index.of(colKeys);
        if (rowCount < windowSize) {
            return DataFrame.ofDoubles(Index.empty(), colIndex);
        } else {
            final int colCount = frame.cols().count();
            final Array<R> rowKeys = frame.rowKeys().toArray(windowSize-1, rowCount);
            final Index<R> rowIndex = Index.of(rowKeys);
            final DataFrame<R,C> result = DataFrame.ofDoubles(rowIndex, colIndex);
            final Statistic1 stdDev = new StdDev(true);
            for (int rowOrdinal=windowSize-1; rowOrdinal<rowCount; ++rowOrdinal) {
                for (int colOrdinal=0; colOrdinal<colCount; ++colOrdinal) {
                    final int from = rowOrdinal-windowSize+1;
                    for (int i=from; i<=rowOrdinal; ++i) {
                        final double value = frame.data().getDouble(i, colOrdinal);
                        if (!Double.isNaN(value)) {
                            stdDev.add(value);
                        }
                    }
                    result.data().setDouble(from, colOrdinal, stdDev.getValue());
                }
            }
            return result;
        }
    }


}
