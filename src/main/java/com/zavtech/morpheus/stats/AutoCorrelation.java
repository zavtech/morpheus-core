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
 * A Statistic implementation that supports incremental calculation of a sample auto correlation
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class AutoCorrelation implements Statistic1 {

    private long n;
    private int lag;
    private int count;
    private double sxy;
    private double sumX;
    private double sumY;
    private double[] memory;
    private Statistic1 stdDev1;
    private Statistic1 stdDev2;

    /**
     * Constructor
     * @param lag   the lag for auto-correlation
     */
    public AutoCorrelation(int lag) {
        this.lag = lag;
        this.memory = new double[lag];
        this.stdDev1 = new StdDev(true);
        this.stdDev2 = new StdDev(true);
    }

    @Override
    public long getN() {
        return n;
    }

    @Override
    public double getValue() {
        if (lag == 0) {
            return 1d;
        } else if (lag >= n) {
            throw new StatException("Lag is too large for sample size, lag of " + lag + " is >= " + n);
        } else {
            final double covariance = sxy / (n-1d);
            final double std1 = stdDev1.getValue();
            final double std2 = stdDev2.getValue();
            return covariance / (std1 * std2);
        }
    }

    @Override
    public StatType getType() {
        return StatType.AUTO_CORREL;
    }

    @Override
    public long add(double value) {
        if (lag != 0) {
            if (count < lag) {
                this.memory[count] = value;
                this.count++;
            } else {
                final double valueLagged = memory[0];
                System.arraycopy(memory, 1, memory, 0, memory.length-1);
                this.memory[lag-1] = value;
                this.stdDev1.add(value);
                this.stdDev2.add(valueLagged);
                if (n == 0) {
                    this.sumX = value;
                    this.sumY = valueLagged;
                    this.n++;
                } else {
                    this.sumX += value;
                    this.sxy += (value - sumX/(n+1d))*(valueLagged - sumY/n);
                    this.sumY += valueLagged;
                    this.n++;
                }
            }
        }
        return n;
    }

    @Override
    public Statistic1 copy() {
        try {
            final AutoCorrelation clone = (AutoCorrelation)super.clone();
            clone.memory = new double[lag];
            clone.stdDev1 = stdDev1.copy();
            clone.stdDev2 = stdDev2.copy();
            return clone;
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException("Failed to clone statistic", ex);
        }
    }

    @Override
    public Statistic1 reset() {
        this.n = 0L;
        this.sxy = 0d;
        this.sumX = 0d;
        this.sumY = 0d;
        this.count = 0;
        this.stdDev1.reset();
        this.stdDev2.reset();
        Arrays.fill(memory, Double.NaN);
        return this;
    }

    public static void main(String[] args) {
        final int lag = 2;
        final double[] sample = {
                0.437471588,
                0.697953703,
                0.952888512,
                0.04129553,
                0.06418075,
                0.534884121,
                0.474263502,
                0.807822886,
                0.749399812,
                0.816661005,
                0.933475064,
                0.485340137
        };

        final Statistic2 corr = new Correlation();
        final Statistic1 autoCorr = new AutoCorrelation(lag);
        final Statistic1 meanStat = new Mean();
        final Statistic1 varStat = new Variance(true);

        for (double v1 : sample) {
            autoCorr.add(v1);
            meanStat.add(v1);
            varStat.add(v1);
        }

        for (int i=lag; i<sample.length; ++i) {
            final double v1 = sample[i-lag];
            final double v2 = sample[i];
            corr.add(v1, v2);
        }

        final double r1 = corr.getValue();
        final double r2 = autoCorr.getValue();
        final long n1 = corr.getN();
        final long n2 = autoCorr.getN();
        System.out.println("R1=" + r1 + ", R2=" + r2);
        if (r1 != r1) throw new RuntimeException("Values to no match");
        if (n1 != n2) throw new RuntimeException("Values to no match");
    }
}
