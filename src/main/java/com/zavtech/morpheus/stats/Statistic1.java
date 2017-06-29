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
 * An interface that defines an incremental calculation of a uni-variate statistic.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public interface Statistic1 extends Statistic {

    /**
     * Adds a new value to the sample for this statistic
     * @param value     the value to add
     * @return          the sample size after adding value
     */
    long add(double value);

    /**
     * Returns a copy of this statistic
     * @return  a copy of this object
     */
    Statistic1 copy();

    /**
     * Resets this statistic back to initial state
     * @return  this statistic
     */
    Statistic1 reset();


    /**
     * Convenience function to compute a univariate statistic on some sample
     * @param stat      the statistic type
     * @param sample    the sample of values
     * @param offset    the offset in sample
     * @param length    the length from offset
     * @return          the stat value
     */
    static double compute(Statistic1 stat, Sample sample, int offset, int length) {
        stat.reset();
        for (int i=0; i<length; ++i) {
            final int index = offset + i;
            final double value = sample.getDouble(index);
            stat.add(value);
        }
        return stat.getValue();
    }

}
