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
package com.zavtech.morpheus.array;

import com.zavtech.morpheus.stats.AutoCorrelation;
import com.zavtech.morpheus.stats.Count;
import com.zavtech.morpheus.stats.GeoMean;
import com.zavtech.morpheus.stats.Kurtosis;
import com.zavtech.morpheus.stats.Max;
import com.zavtech.morpheus.stats.Mean;
import com.zavtech.morpheus.stats.MeanAbsDev;
import com.zavtech.morpheus.stats.Median;
import com.zavtech.morpheus.stats.Min;
import com.zavtech.morpheus.stats.Percentile;
import com.zavtech.morpheus.stats.Product;
import com.zavtech.morpheus.stats.Skew;
import com.zavtech.morpheus.stats.Statistic1;
import com.zavtech.morpheus.stats.Stats;
import com.zavtech.morpheus.stats.StdDev;
import com.zavtech.morpheus.stats.StdErrorMean;
import com.zavtech.morpheus.stats.Sum;
import com.zavtech.morpheus.stats.SumLogs;
import com.zavtech.morpheus.stats.SumSquares;
import com.zavtech.morpheus.stats.Variance;

/**
 * A Stats implementation designed to operate on Morpheus arrays
 *
 * @param <T>   the element type for the Array
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
class ArrayStats<T extends Number> implements Stats<Number> {

    private final Array<T> array;

    /**
     * Constructor
     * @param array     the array to operate on
     */
    ArrayStats(Array<T> array) {
        this.array = array;
    }

    /**
     * Computes the univarite statistic for the array
     * @param stat  the statistic to compute
     * @return      the resulting value
     */
    private Number compute(Statistic1 stat) {
        final int length = array.length();
        for (int i=0; i<length; ++i) {
            final double value = array.getDouble(i);
            if (!Double.isNaN(value)) {
                stat.add(value);
            }
        }
        return stat.getValue();
    }

    @Override
    public final Number count() {
        return compute(new Count());
    }

    @Override
    public final Number min() {
        return compute(new Min());
    }

    @Override
    public final Number max() {
        return compute(new Max());
    }

    @Override
    public final Number mean() {
        return compute(new Mean());
    }

    @Override
    public final Number median() {
        return compute(new Median());
    }

    @Override
    public final Number mad() {
        return compute(new MeanAbsDev(array.length()));
    }

    @Override
    public final Number stdDev() {
        return compute(new StdDev(true));
    }

    @Override
    public final Number sem() {
        return compute(new StdErrorMean());
    }

    @Override
    public final Number sum() {
        return compute(new Sum());
    }

    @Override
    public final Number sumLogs() {
        return compute(new SumLogs());
    }

    @Override
    public final Number sumSquares() {
        return compute(new SumSquares());
    }

    @Override
    public final Number variance() {
        return compute(new Variance(true));
    }

    @Override
    public final Number kurtosis() {
        return compute(new Kurtosis());
    }

    @Override
    public final Number skew() {
        return compute(new Skew());
    }

    @Override
    public final Number geoMean() {
        return compute(new GeoMean());
    }

    @Override
    public final Number product() {
        return compute(new Product());
    }

    @Override
    public final Number autocorr(int lag) {
        return compute(new AutoCorrelation(lag));
    }

    @Override
    public final Number percentile(double nth) {
        return compute(new Percentile(nth));
    }
}
