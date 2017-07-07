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
 * A Statistic implementation that supports incremental calculation of a standard deviation
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class StdDev extends Variance {

    /**
     * Constructor
     * @param biasCorrected true for bias corrected variance
     */
    public StdDev(boolean biasCorrected) {
        super(biasCorrected);
    }

    @Override
    public StatType getType() {
        return StatType.STD_DEV;
    }

    @Override
    public double getValue() {
        return Math.sqrt(super.getValue());
    }
}
