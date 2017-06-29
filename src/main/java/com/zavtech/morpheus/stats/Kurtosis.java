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
 * A Statistic implementation that supports incremental calculation of a sample kurtosis
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class Kurtosis implements Statistic1 {

    private long n;
    private double m1;
    private double m2;
    private double m3;
    private double m4;

    /**
     * Constructor
     */
    public Kurtosis() {
        super();
    }

    @Override
    public long getN() {
        return n;
    }

    @Override
    public double getValue() {
        if (n < 3) {
            return Double.NaN;
        } else {
            final double variance = m2 / (n - 1d);
            if (n <= 3 || variance < 10E-20) {
                return 0d;
            } else {
                final double numerator = (n * (n + 1d) * m4 - 3d * m2 * m2 * (n - 1d));
                final double denominator = ((n - 1d) * (n - 2d) * (n - 3d) * variance * variance);
                return numerator / denominator;
            }
        }
    }

    @Override
    public StatType getType() {
        return StatType.KURTOSIS;
    }

    @Override
    public long add(double value) {
        final double prevM2 = m2;
        final double prevM3 = m3;
        final double dev = value - m1;
        final double nDev = dev / ++n;
        final double nDevSq = nDev * nDev;
        this.m1 += nDev;
        this.m2 += (n - 1d) * dev * nDev;
        this.m3 = m3 - 3d * nDev * prevM2 + (n - 1d) * (n - 2d) * nDevSq * dev;
        this.m4 = m4 - 4d * nDev * prevM3 + 6d * nDevSq * prevM2 + ((n * n) - 3d * (n -1d)) * (nDevSq * nDevSq * (n - 1d) * n);
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
        this.m2 = 0d;
        this.m3 = 0d;
        this.m4 = 0d;
        return this;
    }


    public static void main(String[] args) {
        final double[] values = new java.util.Random().doubles(5000).toArray();
        final Kurtosis stat1 = new Kurtosis();
        final org.apache.commons.math3.stat.descriptive.moment.Kurtosis stat2 = new org.apache.commons.math3.stat.descriptive.moment.Kurtosis();
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
