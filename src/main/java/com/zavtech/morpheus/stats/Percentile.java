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

import java.util.Arrays;

import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.apache.commons.math3.stat.ranking.NaNStrategy;

/**
 * A Statistic implementation that supports incremental calculation of a sample percentile value
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class Percentile implements Statistic1 {

    private int n;
    private double nth;
    private double[] values;

    /**
     * Constructor
     * @param nth   the requested percentile
     */
    public Percentile(double nth) {
        this.nth = nth;
        this.values = new double[1000];
    }


    @Override
    public long getN() {
        return n;
    }

    @Override
    public double getValue() {
        return new org.apache.commons.math3.stat.descriptive.rank.Percentile(nth * 100)
            .withEstimationType(org.apache.commons.math3.stat.descriptive.rank.Percentile.EstimationType.R_7)
            .withNaNStrategy(NaNStrategy.FIXED)
            .evaluate(values, 0, n);
    }

    @Override
    public StatType getType() {
        return StatType.PERCENTILE;
    }

    @Override
    public long add(double value) {
        if (!Double.isNaN(value)) {
            if (++n > values.length) {
                final int oldCapacity = values.length;
                final int newCapacity = oldCapacity + (oldCapacity >> 1);
                final double[] newValues = new double[newCapacity];
                System.arraycopy(values, 0, newValues, 0, values.length);
                this.values = newValues;
                this.values[n-1] = value;
            } else {
                this.values[n-1] = value;
            }
        }
        return n;
    }

    @Override
    public Statistic1 copy() {
        try {
            final Percentile clone = (Percentile)super.clone();
            clone.values = values.clone();
            return clone;
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException("Failed to clone statistic", ex);
        }
    }

    @Override
    public Statistic1 reset() {
        this.n = 0;
        this.values = new double[values.length];
        Arrays.fill(values, Double.NaN);
        return this;
    }

    public static void main(String[] args) {
        final double[] values = new java.util.Random().doubles(5000).toArray();
        final Percentile stat1 = new Percentile(0.5);
        final Median stat2 = new Median();
        for (double value : values) stat1.add(value);
        final double result1 = stat1.getValue();
        final double result2 = stat2.evaluate(values);
        if (result1 != result2) {
            throw new RuntimeException("Error: " + result1 + " != " + result2);
        }
    }

}
