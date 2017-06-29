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

/**
 * An interface to various commonly used calculations on a DataFrame
 *
 * @param <R>   the frame row key type
 * @param <C>   the frame column key type
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameCalculate<R,C> {

    /**
     * Calculates cumulative returns along the row dimension, row(i) / row(0) - 1
     * @return  the frame of cumulative returns, where result = row(i) / row(0) - 1
     */
    DataFrame<R,C> cumReturns();

    /**
     * Calculates log returns along the row dimension, LN(row(i) / row(i-1))
     * @return  the frame of log returns, where result = LN(row(i) / row(i-1))
     */
    DataFrame<R,C> logReturns();

    /**
     * Calculates percentage changes along the row dimension, row(i) / row(i-1) - 1
     * @return  the frame of cumulative returns, where result = row(i) / row(i-1) - 1
     */
    DataFrame<R,C> percentChanges();

    /**
     * Calculates the simple moving average of the columns in this frame
     * @param windowSize    the window size for moving average
     * @return              the simple moving averages of columns
     */
    DataFrame<R,C> sma(int windowSize);

    /**
     * Calculates the exponential moving average of the columns in this frame
     * @param windowSize    the window size for moving average
     * @return              the exponential moving averages of columns
     */
    DataFrame<R,C> ema(int windowSize);

    /**
     * Calculates rolling standard deviation based on the window size specified
     * @param windowSize    the window size for standard deviation
     * @return              the rolling standard deviation of columns
     */
    DataFrame<R,C> stdDev(int windowSize);


}
