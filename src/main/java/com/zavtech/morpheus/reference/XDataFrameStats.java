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

import java.util.function.IntSupplier;

import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.stats.AutoCorrelation;
import com.zavtech.morpheus.stats.Count;
import com.zavtech.morpheus.stats.MeanAbsDev;
import com.zavtech.morpheus.stats.Sample;
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
class XDataFrameStats implements Stats<Double> {

    private Sample sample;
    private IntSupplier size;

    /**
     * Constructor
     * @param sample    the sample to compute statistic over
     */
    XDataFrameStats(Sample sample, IntSupplier size) {
        this.sample = sample;
        this.size = size;
    }

    @Override()
    public Double count() {
        try {
            final int count = size.getAsInt();
            final Statistic1 stat = new Count();
            for (int i=0; i<count; ++i) {
                stat.add(sample.getDouble(i));
            }
            return stat.getValue();
        } catch (StatException ex) {
            throw new DataFrameException(ex.getMessage(), ex);
        }
    }

    @Override()
    public Double min() {
        try {
            final int count = size.getAsInt();
            final Statistic1 stat = new Min();
            for (int i=0; i<count; ++i) {
                stat.add(sample.getDouble(i));
            }
            return stat.getValue();
        } catch (StatException ex) {
            throw new DataFrameException(ex.getMessage(), ex);
        }
    }

    @Override()
    public Double max() {
        try {
            final int count = size.getAsInt();
            final Statistic1 stat = new Max();
            for (int i=0; i<count; ++i) {
                stat.add(sample.getDouble(i));
            }
            return stat.getValue();
        } catch (StatException ex) {
            throw new DataFrameException(ex.getMessage(), ex);
        }
    }

    @Override()
    public Double mean() {
        try {
            final int count = size.getAsInt();
            final Statistic1 stat = new Mean();
            for (int i = 0; i < count; ++i) {
                stat.add(sample.getDouble(i));
            }
            return stat.getValue();
        } catch (StatException ex) {
            throw new DataFrameException(ex.getMessage(), ex);
        }
    }

    @Override()
    public Double stdDev() {
        try {
            final int count = size.getAsInt();
            final Statistic1 stat = new StdDev(true);
            for (int i=0; i<count; ++i) {
                stat.add(sample.getDouble(i));
            }
            return stat.getValue();
        } catch (StatException ex) {
            throw new DataFrameException(ex.getMessage(), ex);
        }
    }

    @Override()
    public Double sum() {
        try {
            final int count = size.getAsInt();
            final Statistic1 stat = new Sum();
            for (int i=0; i<count; ++i) {
                stat.add(sample.getDouble(i));
            }
            return stat.getValue();
        } catch (StatException ex) {
            throw new DataFrameException(ex.getMessage(), ex);
        }
    }

    @Override
    public Double sumLogs() {
        try {
            final int count = size.getAsInt();
            final Statistic1 stat = new SumLogs();
            for (int i=0; i<count; ++i) {
                stat.add(sample.getDouble(i));
            }
            return stat.getValue();
        } catch (StatException ex) {
            throw new DataFrameException(ex.getMessage(), ex);
        }
    }

    @Override()
    public Double sumSquares() {
        try {
            final int count = size.getAsInt();
            final Statistic1 stat = new SumSquares();
            for (int i=0; i<count; ++i) {
                stat.add(sample.getDouble(i));
            }
            return stat.getValue();
        } catch (StatException ex) {
            throw new DataFrameException(ex.getMessage(), ex);
        }
    }

    @Override()
    public Double variance() {
        try {
            final int count = size.getAsInt();
            final Statistic1 stat = new Variance(true);
            for (int i=0; i<count; ++i) {
                stat.add(sample.getDouble(i));
            }
            return stat.getValue();
        } catch (StatException ex) {
            throw new DataFrameException(ex.getMessage(), ex);
        }
    }

    @Override()
    public Double skew() {
        try {
            final int count = size.getAsInt();
            final Statistic1 stat = new Skew();
            for (int i=0; i<count; ++i) {
                stat.add(sample.getDouble(i));
            }
            return stat.getValue();
        } catch (StatException ex) {
            throw new DataFrameException(ex.getMessage(), ex);
        }
    }

    @Override()
    public Double kurtosis() {
        try {
            final int count = size.getAsInt();
            final Statistic1 stat = new Kurtosis();
            for (int i=0; i<count; ++i) {
                stat.add(sample.getDouble(i));
            }
            return stat.getValue();
        } catch (StatException ex) {
            throw new DataFrameException(ex.getMessage(), ex);
        }
    }

    @Override()
    public Double geoMean() {
        try {
            final int count = size.getAsInt();
            final Statistic1 stat = new GeoMean();
            for (int i=0; i<count; ++i) {
                stat.add(sample.getDouble(i));
            }
            return stat.getValue();
        } catch (StatException ex) {
            throw new DataFrameException(ex.getMessage(), ex);
        }
    }

    @Override()
    public Double product() {
        try {
            final int count = size.getAsInt();
            final Statistic1 stat = new Product();
            for (int i=0; i<count; ++i) {
                stat.add(sample.getDouble(i));
            }
            return stat.getValue();
        } catch (StatException ex) {
            throw new DataFrameException(ex.getMessage(), ex);
        }
    }

    @Override()
    public Double median() {
        try {
            final int count = size.getAsInt();
            final Statistic1 stat = new Median();
            for (int i=0; i<count; ++i) {
                stat.add(sample.getDouble(i));
            }
            return stat.getValue();
        } catch (Exception ex) {
            throw new DataFrameException("Failed to compute median", ex);
        }
    }

    @Override
    public Double mad() {
        try {
            final int count = size.getAsInt();
            final Statistic1 stat = new MeanAbsDev(count);
            for (int i=0; i<count; ++i) {
                stat.add(sample.getDouble(i));
            }
            return stat.getValue();
        } catch (Exception ex) {
            throw new DataFrameException("Failed to compute mean absolute deviation", ex);
        }
    }

    @Override
    public Double sem() {
        try {
            final int count = size.getAsInt();
            final Statistic1 stat = new StdErrorMean();
            for (int i=0; i<count; ++i) {
                stat.add(sample.getDouble(i));
            }
            return stat.getValue();
        } catch (Exception ex) {
            throw new DataFrameException("Failed to compute standard error of the mean", ex);
        }
    }

    @Override()
    public Double autocorr(int lag) {
        try {
            final int count = size.getAsInt();
            final Statistic1 stat = new AutoCorrelation(lag);
            for (int i=0; i<count; ++i) {
                stat.add(sample.getDouble(i));
            }
            return stat.getValue();
        } catch (Exception ex) {
            throw new DataFrameException("Failed to compute auto correlation", ex);
        }
    }

    @Override()
    public Double percentile(double nth) {
        try {
            final int count = size.getAsInt();
            final Statistic1 stat = new Percentile(nth);
            for (int i=0; i<count; ++i) {
                stat.add(sample.getDouble(i));
            }
            return stat.getValue();
        } catch (Exception ex) {
            throw new DataFrameException("Failed to compute percentile", ex);
        }
    }
}
