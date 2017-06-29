package com.zavtech.morpheus.stats;

import java.util.HashMap;
import java.util.Map;

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
