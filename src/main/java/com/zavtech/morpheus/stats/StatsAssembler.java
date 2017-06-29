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

/**
 * A convenience base class for build components that compute bulk statistics and return some structure of type T.
 *
 * @param <T>   the output type produced by this assembler
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public abstract class StatsAssembler<T> implements Stats<T> {

    /**
     * Constructor
     */
    public StatsAssembler() {
        super();
    }

    /**
     * Computes a result based on the univariate statistic
     * @param stat  the univariate statistic
     * @return      the results
     */
    protected abstract T compute(Statistic1 stat);


    @Override
    public T count() {
        return compute(new Count());
    }

    @Override
    public T min() {
        return compute(new Min());
    }

    @Override
    public T max() {
        return compute(new Max());
    }

    @Override
    public T mean() {
        return compute(new Mean());
    }

    @Override
    public T median() {
        return compute(new Median());
    }

    @Override
    public T mad() {
        return compute(new MeanAbsDev());
    }

    @Override
    public T stdDev() {
        return compute(new StdDev(true));
    }

    @Override
    public T sem() {
        return compute(new StdErrorMean());
    }

    @Override
    public T sum() {
        return compute(new Sum());
    }

    @Override
    public T sumLogs() {
        return compute(new SumLogs());
    }

    @Override
    public T sumSquares() {
        return compute(new SumSquares());
    }

    @Override
    public T variance() {
        return compute(new Variance(true));
    }

    @Override
    public T kurtosis() {
        return compute(new Kurtosis());
    }

    @Override
    public T skew() {
        return compute(new Skew());
    }

    @Override
    public T geoMean() {
        return compute(new GeoMean());
    }

    @Override
    public T product() {
        return compute(new Product());
    }

    @Override
    public T autocorr(int lag) {
        return compute(new AutoCorrelation(lag));
    }

    @Override
    public T percentile(double nth) {
        return compute(new Percentile(nth));
    }
}
