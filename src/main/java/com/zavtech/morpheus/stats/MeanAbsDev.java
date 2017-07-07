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

/**
 * A Statistic implementation that supports incremental calculation of sample Mean Absolute Deviation (MAD)
 *
 * @see <a href="https://en.wikipedia.org/wiki/Average_absolute_deviation">Wikipedia</a>
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class MeanAbsDev implements Statistic1 {

    private int n;
    private double m1;
    private double[] sample;

    /**
     * Constructor
     */
    public MeanAbsDev() {
        this(1000);
    }

    /**
     * Constructor
     * @param size  the max size of sample to support
     */
    public MeanAbsDev(int size) {
        this.sample = new double[size];
    }

    @Override
    public long getN() {
        return n;
    }

    @Override
    public double getValue() {
        final int n = (int)getN();
        if (n == 0) {
            return Double.NaN;
        } else if (n == 1) {
            return 0d;
        } else {
            double result = 0d;
            for (int i=0; i<n; ++i) {
                final double value = sample[i];
                final double dev = Math.abs(value - m1);
                result += (dev - result) / (i+1);
            }
            return result;
        }
    }

    @Override
    public StatType getType() {
        return StatType.MAD;
    }

    @Override
    public long add(double value) {
        if (!Double.isNaN(value)) {
            this.m1 += (value - m1) / ++n;
            this.sample[n-1] = value;
            //todo: grow the array if necessary
        }
        return n;
    }

    @Override
    public Statistic1 copy() {
        try {
            final MeanAbsDev clone = (MeanAbsDev)super.clone();
            clone.sample = new double[sample.length];
            Arrays.fill(clone.sample, Double.NaN);
            return clone;
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException("Failed to clone statistic", ex);
        }
    }

    @Override()
    public Statistic1 reset() {
        this.n = 0;
        this.m1 = 0d;
        Arrays.fill(sample, Double.NaN);
        return this;
    }

    public static void main(String[] args) {
        final double[] values = new double[] {
                20.69424224,
                82.70249213,
                15.97904565,
                62.43902949,
                64.58867298,
                46.06848044,
                66.44973931,
                94.01121918,
                8.814351706,
                16.35090148,
                77.28782068,
                92.77535333,
                52.02176958,
                5.203382676,
                58.98593825,
                4.340973592,
                24.36451039
        };

        final MeanAbsDev stat = new MeanAbsDev(values.length);
        for (double value : values) stat.add(value);
        final double result = stat.getValue();
        System.out.println("V1=" + result + ", V2=27.22320613");
    }
}
