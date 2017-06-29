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
 * A bi-variate Statistic implementation that supports incremental calculation of a sample covariance.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class Covariance implements Statistic2 {

    private long n;
    private double sxy;
    private double sumX;
    private double sumY;

    /**
     * Constructor
     */
    public Covariance() {
        super();
    }

    @Override
    public long getN() {
        return n;
    }

    @Override
    public double getValue() {
        return sxy / (n-1d);
    }

    @Override
    public StatType getType() {
        return StatType.COVARIANCE;
    }

    @Override
    public long add(double value1, double value2) {
        if (n == 0) {
            this.sumX = value1;
            this.sumY = value2;
            this.n++;
        } else {
            this.sumX += value1;
            this.sxy += (value1 - sumX/(n+1d))*(value2 - sumY/n);
            this.sumY += value2;
            this.n++;
        }
        return n;
    }

    @Override
    public Statistic2 copy() {
        try {
            return (Statistic2)super.clone();
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException("Failed to clone statistic", ex);
        }
    }

    @Override
    public Statistic2 reset() {
        this.n = 0L;
        this.sxy = 0d;
        this.sumX = 0d;
        this.sumY = 0d;
        return this;
    }
}
