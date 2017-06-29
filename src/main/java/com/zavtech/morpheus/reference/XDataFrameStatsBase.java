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

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;
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
 * A convenience base class for building Stats implementations that return bulk statistics
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
abstract class XDataFrameStatsBase<R,C> implements Stats<DataFrame<R,C>> {

    private boolean columns;
    private boolean parallel;

    /**
     * Constructor
     * @param columns       true if operating on columns
     * @param parallel      true if should operate in parallel
     */
    XDataFrameStatsBase(boolean columns, boolean parallel) {
        this.columns = columns;
        this.parallel = parallel;
    }

    /**
     * Returns the row count for frame
     * @return  the row count
     */
    protected abstract int rowCount();

    /**
     * Returns the column count for frame
     * @return  the column count
     */
    protected abstract int colCount();

    /**
     * Returns true is operates in parallel mode
     * @return      true if operates in parallel mode
     */
    public boolean isParallel() {
        return parallel;
    }


    @Override
    public DataFrame<R,C> count() {
        return compute(new Count());
    }


    @Override
    public DataFrame<R,C> min() {
        return compute(new Min());
    }


    @Override
    public DataFrame<R,C> max() {
        return compute(new Max());
    }


    @Override
    public DataFrame<R,C> mean() {
        return compute(new Mean());
    }


    @Override
    public DataFrame<R,C> median() {
        return compute(new Median());
    }


    @Override
    public DataFrame<R,C> mad() {
        return compute(columns ? new MeanAbsDev(rowCount()): new MeanAbsDev(colCount()));
    }


    @Override
    public DataFrame<R,C> stdDev() {
        return compute(new StdDev(true));
    }


    @Override
    public DataFrame<R, C> sem() {
        return compute(new StdErrorMean());
    }


    @Override
    public DataFrame<R,C> sum() {
        return compute(new Sum());
    }


    @Override
    public DataFrame<R, C> sumLogs() {
        return compute(new SumLogs());
    }


    @Override
    public DataFrame<R, C> sumSquares() {
        return compute(new SumSquares());
    }


    @Override
    public DataFrame<R,C> variance() {
        return compute(new Variance(true));
    }


    @Override
    public DataFrame<R,C> kurtosis() {
        return compute(new Kurtosis());
    }


    @Override
    public DataFrame<R,C> skew() {
        return compute(new Skew());
    }


    @Override
    public DataFrame<R,C> geoMean() {
        return compute(new GeoMean());
    }


    @Override
    public DataFrame<R,C> product() {
        return compute(new Product());
    }


    @Override
    public DataFrame<R, C> autocorr(int lag) {
        return compute(new AutoCorrelation(lag));
    }


    @Override
    public DataFrame<R,C> percentile(double nth) {
        return compute(new Percentile(nth));
    }

    /**
     * Returns true if the operation is viable
     * @param statistic     the statistic instance
     * @return          true if viable
     */
    protected abstract boolean isViable(Statistic1 statistic);

    /**
     * Returns the DataFrame to capture results of the statistic function
     * @param statistic     the statistic instance
     * @param viable        true if the calculation is viable
     * @return              the resulting DataFrame
     */
    protected abstract XDataFrame<R,C> createResult(Statistic1 statistic, boolean viable);

    /**
     * Returns a statistic recursive action for the arguments provided
     * @param statistic     the statistic instance
     * @param result        the DataFrame to write results to
     * @return              the newly created recursive action
     */
    protected abstract StatisticAction createStatisticAction(Statistic1 statistic, XDataFrame<R,C> result);


    /**
     * Returns a DataFrame containing results based on the statistic specified
     * @param statistic     the statistic instance
     * @return              the resulting DataFrame
     */
    protected DataFrame<R,C> compute(Statistic1 statistic) {
        try {
            final boolean viable = isViable(statistic);
            final XDataFrame<R,C> result = createResult(statistic, viable);
            if (!viable) {
                return result;
            } else {
                final StatisticAction action = createStatisticAction(statistic, result);
                if (isParallel()) {
                    ForkJoinPool.commonPool().invoke(action);
                } else {
                    action.compute();
                }
                return result;
            }
        } catch (Exception ex) {
            throw new DataFrameException("Failed to compute statistic on DataFrame", ex);
        }
    }


    /**
     * Package access inner class that extends RecursiveAction to make compute() accessible.
     */
    static abstract class StatisticAction extends RecursiveAction {

        @Override
        public abstract void compute();
    }

}
