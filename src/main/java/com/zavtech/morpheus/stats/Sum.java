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
 * A Statistic implementation that supports incremental calculation of a sample sum
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class Sum implements Statistic1 {

    private long n;
    private double sum;

    /**
     * Constructor
     */
    public Sum() {
        super();
    }

    @Override
    public long getN() {
        return n;
    }

    @Override
    public double getValue() {
        return sum;
    }

    @Override
    public StatType getType() {
        return StatType.SUM;
    }

    @Override
    public long add(double value) {
        if (!Double.isNaN(value)) {
            this.sum += value;
            this.n++;
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

    @Override
    public Statistic1 reset() {
        this.n = 0L;
        this.sum = 0d;
        return this;
    }
}
