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
 * A bi-variate Statistic implementation that supports incremental calculation of a sample correlation.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class Correlation extends Covariance {

    private Statistic1 stdDev1;
    private Statistic1 stdDev2;

    /**
     * Constructor
     */
    public Correlation() {
        this.stdDev1 = new StdDev(true);
        this.stdDev2 = new StdDev(true);
    }

    @Override
    public double getValue() {
        final double covariance = super.getValue();
        final double std1 = stdDev1.getValue();
        final double std2 = stdDev2.getValue();
        return covariance / (std1 * std2);
    }

    @Override
    public StatType getType() {
        return StatType.CORRELATION;
    }

    @Override
    public long add(double value1, double value2) {
        super.add(value1, value2);
        this.stdDev1.add(value1);
        this.stdDev2.add(value2);
        return getN();
    }

    @Override
    public Statistic2 copy() {
        try {
            final Correlation clone = (Correlation)super.clone();
            clone.stdDev1 = stdDev1.copy();
            clone.stdDev2 = stdDev2.copy();
            return clone;
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException("Failed to clone statistic", ex);
        }
    }

    @Override
    public Statistic2 reset() {
        super.reset();
        this.stdDev1.reset();
        this.stdDev2.reset();
        return this;
    }
}
