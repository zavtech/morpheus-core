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
import com.zavtech.morpheus.frame.DataFrameValue;
import com.zavtech.morpheus.stats.AutoCorrelation;
import com.zavtech.morpheus.stats.Count;
import com.zavtech.morpheus.stats.MeanAbsDev;
import com.zavtech.morpheus.stats.StatException;
import com.zavtech.morpheus.stats.GeoMean;
import com.zavtech.morpheus.stats.Kurtosis;
import com.zavtech.morpheus.stats.Max;
import com.zavtech.morpheus.stats.Mean;
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
 * An implementation of the Stats interface that operates on some sample.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class XDataFrameStats<R,C> implements Stats<Double> {

    private boolean skipNaNs;
    private Iterable<DataFrameValue<R,C>> values;

    /**
     * Constructor
     * @param skipNaNs  if true, skip NaN values
     * @param values    the values to compute stats over
     */
    XDataFrameStats(boolean skipNaNs, Iterable<DataFrameValue<R,C>> values) {
        this.skipNaNs = skipNaNs;
        this.values = values;
    }

    /**
     * Computes the uni-variate statistic specified over the sample
     * @param statistic     the statistic to compute
     * @return              the resulting statistic value
     */
    private Double compute(Statistic1 statistic) {
        try {
            this.values.forEach(value -> {
                if (value.isNumeric()) {
                    final double doubleValue = value.getDouble();
                    if (!skipNaNs || !Double.isNaN(doubleValue)) {
                        statistic.add(doubleValue);
                    }
                }
            });
            return statistic.getValue();
        } catch (StatException ex) {
            throw new DataFrameException(ex.getMessage(), ex);
        }
    }

    @Override()
    public final Double count() {
        return compute(new Count());
    }

    @Override()
    public final Double min() {
        return compute(new Min());
    }

    @Override()
    public final Double max() {
        return compute(new Max());
    }

    @Override()
    public final Double mean() {
        return compute(new Mean());
    }

    @Override()
    public final Double stdDev() {
        return compute(new StdDev(true));
    }

    @Override()
    public final Double sum() {
        return compute(new Sum());
    }

    @Override
    public final Double sumLogs() {
        return compute(new SumLogs());
    }

    @Override()
    public final Double sumSquares() {
        return compute(new SumSquares());
    }

    @Override()
    public final Double variance() {
        return compute(new Variance(true));
    }

    @Override()
    public final Double skew() {
        return compute(new Skew());
    }

    @Override()
    public final Double kurtosis() {
        return compute(new Kurtosis());
    }

    @Override()
    public final Double geoMean() {
        return compute(new GeoMean());
    }

    @Override()
    public final Double product() {
        return compute(new Product());
    }

    @Override()
    public final Double median() {
        return compute(new Median());
    }

    @Override
    public final Double mad() {
        return compute(new MeanAbsDev());
    }

    @Override
    public final Double sem() {
        return compute(new StdErrorMean());
    }

    @Override()
    public final Double autocorr(int lag) {
        return compute(new AutoCorrelation(lag));
    }

    @Override()
    public final Double percentile(double nth) {
        return compute(new Percentile(nth));
    }
}
