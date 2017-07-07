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
package com.zavtech.morpheus.stats;

/**
 * An interface to an object that provides summary statistics on itself.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public interface Stats<T> {

    /**
     * Returns the number of non null values
     * @return      the number of non null values
     */
    T count();

    /**
     * Returns the minimum value for this entity
     * @return      the minimum value
     * @see <a href="https://en.wikipedia.org/wiki/Sample_maximum_and_minimum">Wikipedia</a>
     */
    T min();

    /**
     * Returns the maximum value for this entity
     * @return      the maximum value
     * @see <a href="https://en.wikipedia.org/wiki/Sample_maximum_and_minimum">Wikipedia</a>
     */
    T max();

    /**
     * Returns the arithmetic mean value for this entity
     * @return      the mean value
     * @see <a href="http://en.wikipedia.org/wiki/Arithmetic_mean">Wikipedia</a>
     */
    T mean();

    /**
     * Returns the median value for this entity
     * @return      the median value
     * @see <a href="http://en.wikipedia.org/wiki/Median">Wikipedia</a>
     */
    T median();

    /**
     * Returns the Mean Absolute Deviation for this entity
     * @return  the Mean Absolute Deviation
     * @see <a href="https://en.wikipedia.org/wiki/Average_absolute_deviation">Wikipedia</a>
     */
    T mad();

    /**
     * Returns the sample standard deviation for this entity
     * @return      the sample standard deviation
     * @see <a href="http://en.wikipedia.org/wiki/Standard_deviation">Wikipedia</a>
     */
    T stdDev();

    /**
     * Returns the Standard Error of the Mean for this entity
     * @return  the Standard Error of the Mean
     * @see <a href="https://en.wikipedia.org/wiki/Standard_error">Wikipedia</a>
     */
    T sem();

    /**
     * Returns the sum of values for this entity
     * @return      the sum
     * @see <a href="http://en.wikipedia.org/wiki/Sum">Wikipedia</a>
     */
    T sum();

    /**
     * Returns the sum of the logs for this entity
     * @return      the sum of logs
     * @see <a href="http://en.wikipedia.org/wiki/Sum">Wikipedia</a>
     */
    T sumLogs();

    /**
     * Returns the sum of the squares for this entity
     * @return      the sum of squares
     * @see <a href="http://en.wikipedia.org/wiki/Sum">Wikipedia</a>
     */
    T sumSquares();

    /**
     * Returns the sample variance for this entity
     * @return      the sample variance
     * @see <a href="http://en.wikipedia.org/wiki/Variance">Wikipedia</a>
     */
    T variance();

    /**
     * Returns the Kurtosis for this entity
     * @return      the kurtosis
     * @see <a href="http://en.wikipedia.org/wiki/Kurtosis">Wikipedia</a>
     */
    T kurtosis();

    /**
     * Returns the skewness for this entity
     * @return      the skewness
     * @see <a href="http://en.wikipedia.org/wiki/Skewness">Wikipedia</a>
     */
    T skew();

    /**
     * Returns the geometric mean for this entity
     * @return  the geometric mean, NaN if the product of the available values is less than or equal to 0.
     * @see <a href="http://en.wikipedia.org/wiki/Geometric_mean">Wikipedia</a>
     */
    T geoMean();

    /**
     * Returns the product for this entity
     * @return  the geometric mean, NaN if the product of the available values is less than or equal to 0.
     * @see <a href="http://en.wikipedia.org/wiki/Geometric_mean">Wikipedia</a>
     */
    T product();

    /**
     * Returns the auto correlation for this entity
     * @param lag   the number of periods to lag
     * @return      the auto correlation statistic
     */
    T autocorr(int lag);

    /**
     * Returns an estimate for the nth percentile for this entity
     * @param nth   the requested percentile (scaled from 0 - 100)
     * @return      estimate for the nth percentile of row or column
     * @see <a href="http://en.wikipedia.org/wiki/Percentile">Wikipedia</a>
     */
    T percentile(double nth);

}
