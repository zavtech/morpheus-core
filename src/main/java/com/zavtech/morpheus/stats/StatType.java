/**
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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.zavtech.morpheus.frame.DataFrameException;

/**
 * An enum that defines various statistic types
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public enum StatType {

    SUM,
    MIN,
    MAX,
    MAD,
    SEM,
    MEAN,
    COUNT,
    MEDIAN,
    PRODUCT,
    PERCENTILE,
    SUM_LOGS,
    SUM_SQUARES,
    STD_DEV,
    VARIANCE,
    KURTOSIS,
    SKEWNESS,
    GEO_MEAN,
    COVARIANCE,
    CORRELATION,
    AUTO_CORREL;


    private static List<StatType> univariate = Collections.unmodifiableList(Arrays.asList(
        SUM, MIN, MAX, MAD, SEM, MEAN, COUNT, MEDIAN, PERCENTILE, SUM_LOGS, SUM_SQUARES, STD_DEV, VARIANCE, KURTOSIS, SKEWNESS, GEO_MEAN, AUTO_CORREL
    ));


    /**
     * Returns the universe of univariate stat types supported
     * @return  the universe of univariate stat types
     */
    public static List<StatType> univariate() {
        return univariate;
    }


    /**
     * Returns the stat value for this type
     * @param stats     the stats entity
     * @return          the stat value
     */
    public double apply(Stats<Double> stats) {
        switch (this) {
            case MIN:               return stats.min();
            case MAX:               return stats.max();
            case MEAN:              return stats.mean();
            case MEDIAN:            return stats.median();
            case MAD:               return stats.mad();
            case SUM:               return stats.sum();
            case SUM_LOGS:          return stats.sumLogs();
            case SEM:               return stats.sem();
            case STD_DEV:           return stats.stdDev();
            case VARIANCE:          return stats.variance();
            case KURTOSIS:          return stats.kurtosis();
            case SKEWNESS:          return stats.skew();
            case GEO_MEAN:          return stats.geoMean();
            case COUNT:             return stats.count();
            case PRODUCT:           return stats.product();
            case SUM_SQUARES:       return stats.sumSquares();
            case PERCENTILE:        return stats.percentile(0.5d);
            case AUTO_CORREL:  return stats.autocorr(1);
            default:    throw new DataFrameException("Unsupported stat type: " + this.name());
        }
    }



    }
