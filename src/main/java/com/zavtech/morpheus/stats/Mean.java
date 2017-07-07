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
 * A Statistic implementation that supports incremental calculation of a sample mean
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class Mean implements Statistic1 {

    private long n;
    private double m1;

    /**
     * Constructor
     */
    public Mean() {
        super();
    }

    @Override
    public long getN() {
        return n;
    }

    @Override
    public double getValue() {
        return m1;
    }

    @Override
    public StatType getType() {
        return StatType.MEAN;
    }

    @Override
    public long add(double value) {
        if (!Double.isNaN(value)) {
            this.m1 += (value - m1) / ++n;
        }
        return n;
    }

    @Override
    public Statistic1 copy() {
        try {
            return (Statistic1)super.clone();
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException("Failed to clone statistic", ex);
        }
    }

    @Override()
    public Statistic1 reset() {
        this.n = 0L;
        this.m1 = 0d;
        return this;
    }


    public static void main(String[] args) {
        final double[] values = new java.util.Random().doubles(5000).toArray();
        final Mean stat1 = new Mean();
        final org.apache.commons.math3.stat.descriptive.moment.Mean stat2 = new org.apache.commons.math3.stat.descriptive.moment.Mean();
        for (double value : values) {
            stat1.add(value);
            stat2.increment(value);
        }
        final double result1 = stat1.getValue();
        final double result2 = stat2.getResult();
        if (result1 != result2) {
            throw new RuntimeException("Error: " + result1 + " != " + result2);
        }
    }
}
