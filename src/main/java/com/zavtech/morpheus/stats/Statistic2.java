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
 * An interface that defines an incremental calculation of a bi-variate statistic.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public interface Statistic2 extends Statistic {

    /**
     * Adds a new pair of values to the sample for this bi-variate statistic
     * @param value1    the value from first sample
     * @param value2    the value from second sample
     * @return          the sample size after adding value
     */
    long add(double value1, double value2);

    /**
     * Returns a copy of this statistic
     * @return  a copy of this object
     */
    Statistic2 copy();

    /**
     * Resets this statistic back to initial state
     * @return  this statistic
     */
    Statistic2 reset();

}
