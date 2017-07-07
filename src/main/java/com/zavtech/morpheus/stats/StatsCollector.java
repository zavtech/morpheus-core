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

import java.util.HashMap;
import java.util.Map;

/**
 * A class that can compute multiple stats as values are added.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class StatsCollector {

    private Map<StatType,Statistic1> statMap = new HashMap<>();

    private StatsCollector(Statistic1... stats) {
        for (Statistic1 stat : stats) {
            statMap.put(stat.getType(), stat);
        }
    }

    /**
     * Returns a newly created collector for the stats specified
     * @param stats the stats to collect
     * @return      the newly created collector
     */
    public static StatsCollector of(Statistic1... stats) {
        return new StatsCollector(stats);
    }

    /**
     * Adds the values to all stats in this collector
     * @param value     the observation to add
     * @return          the sample size after adding value
     */
    public long add(double value) {
        return statMap.values().stream().mapToLong(stat -> stat.add(value)).max().orElse(0L);
    }

    /**
     * Resets all the stats in this collector
     */
    public void reset() {
        statMap.values().forEach(Statistic1::reset);
    }

    /**
     * Returns the stat value for the type specified
     * @param type  the stat type
     * @return      the stat value for type
     */
    public double getValue(StatType type) {
        final Statistic1 stat = statMap.get(type);
        if (stat != null) {
            return stat.getValue();
        } else {
            throw new IllegalArgumentException("No statistic exists for " + type + " in this collector");
        }
    }
}
