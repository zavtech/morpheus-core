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

import com.zavtech.morpheus.stats.Stats;

/**
 * An interface that provides an API to query axis level statistics of a DataFrame.
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameAxisStats<K,R,C,X,Y> extends Stats<DataFrame<X,Y>> {

    /**
     * Returns the covariance matrix for all vectors in this axis
     * @return      the covariance matrix between all vectors in this axis
     */
    DataFrame<K,K> covariance();

    /**
     * Returns the correlation matrix for all vectors in this axis
     * @return      the correlation matrix between all vectors in this axis
     */
    DataFrame<K,K> correlation();

    /**
     * Returns the covariance between two vectors on this axis
     * @param key1  the key to first vector
     * @param key2  the key to second vector
     * @return      the covariance between specified vectors
     */
    double covariance(K key1, K key2);

    /**
     * Returns the correlation between two vectors on this axis
     * @param key1  the key to first vector
     * @param key2  the key to second vector
     * @return      the correlation between specified vectors
     */
    double correlation(K key1, K key2);

    /**
     * Returns the Exponential-Weighted Moving Average (EWM) of the values in this dimension
     * @param halfLife  the half-life such that the EWM weight, alpha = 1 - exp(log(0.5)/halfLife)
     * @return          the DataFrame with the EWM smoothed data
     */
    DataFrame<R,C> ewma(int halfLife);

    /**
     * Returns the Exponential-Weighted Moving Standard Deviation of values in this dimension
     * @param halfLife  the half-life such that the EWM weight, alpha = 1 - exp(log(0.5)/halfLife)
     * @return          the DataFrame with the EWM filtered data
     */
    DataFrame<R,C> ewmstd(int halfLife);

    /**
     * Returns the Exponential-Weighted Moving Variance of values in this dimension
     * @param halfLife  the half-life such that the EWM weight, alpha = 1 - exp(log(0.5)/halfLife)
     * @return          the DataFrame with the EWM filtered data
     */
    DataFrame<R,C> ewmvar(int halfLife);

    /**
     * Returns the interface to rolling window statistics in this dimension
     * @param windowSize    the window size for rolling period
     * @return              the rolling window statistical interface
     */
    Stats<DataFrame<R,C>> rolling(int windowSize);

    /**
     * Returns the interface to expanding window statistics in this dimension
     * @param minPeriods    the min number of periods to produce a statistic
     * @return              the expanding window statistical interface
     */
    Stats<DataFrame<R,C>> expanding(int minPeriods);

}
