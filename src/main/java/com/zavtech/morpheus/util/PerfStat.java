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
package com.zavtech.morpheus.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.stats.Max;
import com.zavtech.morpheus.stats.Mean;
import com.zavtech.morpheus.stats.Median;
import com.zavtech.morpheus.stats.Min;
import com.zavtech.morpheus.stats.StatType;
import com.zavtech.morpheus.stats.StatsCollector;
import com.zavtech.morpheus.stats.StdDev;

/**
 * A utility class to facilitate capturing performance statistics on blocks of code.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class PerfStat implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private static MemoryEstimator memoryEstimator;

    private int count;
    private String label;
    private TimeUnit units;
    private StatsCollector memoryStats;
    private StatsCollector gcTimeStats;
    private StatsCollector callTimeStats;
    private DataFrame<String,String> frame;
    private List<Runnable> beforeEach;
    private List<Runnable> afterEach;


    /**
     * Constructor
     * @param label the label for this monitor
     */
    public PerfStat(String label) {
        this(label, TimeUnit.MILLISECONDS);
    }

    /**
     * Constructor
     * @param label the label for this monitor
     * @param units the units for timing stats
     */
    public PerfStat(String label, TimeUnit units) {
        this.label = label;
        this.units = units;
        this.beforeEach = new ArrayList<>();
        this.afterEach = new ArrayList<>();
    }

    /**
     * Sets the memory estimator for this process
     * @param memoryEstimator   the memory estimator
     */
    public static void setMemoryEstimator(MemoryEstimator memoryEstimator) {
        PerfStat.memoryEstimator = memoryEstimator;
    }

    /**
     * Returns the label for this performance monitor
     * @return  the label for this monitor
     */
    public String getLabel() {
        return label;
    }

    /**
     * Returns the units for this timing
     * @return      the units for timing
     */
    public TimeUnit getUnits() {
        return units;
    }

    /**
     * Returns the count for this timing
     * @return  the count
     */
    public int getCount() {
        return count;
    }

    /**
     * Returns the GC time stat collected by this performance monitor
     * @param stat  the requested statistic
     * @return      the GC time stat
     */
    public double getGcTime(StatType stat) {
        return gcTimeStats.getValue(stat);
    }

    /**
     * Returns the call time stat collected by this performance monitor
     * @param stat  the requested statistic
     * @return      the call time stat
     */
    public double getCallTime(StatType stat) {
        return callTimeStats.getValue(stat);
    }

    /**
     * Returns the used memory stat collected by this performance monitor
     * @param stat  the requested statistic
     * @return      the used memory stat
     */
    public double getUsedMemory(StatType stat) {
        return memoryStats.getValue(stat);
    }

    /**
     * Returns a DataFrame with the GC time stats for this performance monitor
     * @return  the GC time stats for this performance monitor
     */
    public DataFrame<String,String> getGcStats() {
        final Array<String> categories = Array.of("Min", "Max", "Mean", "Median", "Stdev");
        return DataFrame.ofDoubles(categories, Collections.singleton(label)).applyDoubles(v -> {
            switch (v.rowOrdinal()) {
                case 0:  return getGcTime(StatType.MIN);
                case 1:  return getGcTime(StatType.MAX);
                case 2:  return getGcTime(StatType.MEAN);
                case 3:  return getGcTime(StatType.MEDIAN);
                case 4:  return getGcTime(StatType.STD_DEV);
                default: throw new IllegalArgumentException("Unexpected row count");
            }
        });
    }

    /**
     * Returns a DataFrame with the call time stats for this performance monitor
     * @return  the call time stats for this performance monitor
     */
    public DataFrame<String,String> getRunStats() {
        final Array<String> categories = Array.of("Min", "Max", "Mean", "Median", "Stdev");
        return DataFrame.ofDoubles(categories, Collections.singleton(label)).applyDoubles(v -> {
            switch (v.rowOrdinal()) {
                case 0:  return getCallTime(StatType.MIN);
                case 1:  return getCallTime(StatType.MAX);
                case 2:  return getCallTime(StatType.MEAN);
                case 3:  return getCallTime(StatType.MEDIAN);
                case 4:  return getCallTime(StatType.STD_DEV);
                default: throw new IllegalArgumentException("Unexpected row count");
            }
        });
    }


    /**
     * Returns a DataFrame with the call time stats for this performance monitor
     * @param includeGC true to include GC times in performance stats
     * @return  the call time stats for this performance monitor
     */
    public DataFrame<String,String> getTimeStats(boolean includeGC) {
        final Array<String> categories = Array.of("Min", "Max", "Mean", "Median", "Stdev");
        return DataFrame.ofDoubles(categories, Collections.singleton(label)).applyDoubles(v -> {
            switch (v.rowOrdinal()) {
                case 0:  return getCallTime(StatType.MIN) + (includeGC ? getGcTime(StatType.MIN) : 0);
                case 1:  return getCallTime(StatType.MAX)+ (includeGC ? getGcTime(StatType.MAX) : 0);
                case 2:  return getCallTime(StatType.MEAN)+ (includeGC ? getGcTime(StatType.MEAN) : 0);
                case 3:  return getCallTime(StatType.MEDIAN)+ (includeGC ? getGcTime(StatType.MEDIAN) : 0);
                case 4:  return getCallTime(StatType.STD_DEV)+ (includeGC ? getGcTime(StatType.STD_DEV) : 0);
                default: throw new IllegalArgumentException("Unexpected row count");
            }
        });
    }


    /**
     * Returns a DataFrame with the used memory stats for this performance monitor
     * @return  the used memory stats for this performance monitor
     */
    public DataFrame<String,String> getUsedMemStats() {
        final Array<String> categories = Array.of("Min", "Max", "Mean", "Median", "Stdev");
        return DataFrame.ofDoubles(categories, Collections.singleton(label)).applyDoubles(v -> {
            switch (v.rowOrdinal()) {
                case 0:  return getUsedMemory(StatType.MIN);
                case 1:  return getUsedMemory(StatType.MAX);
                case 2:  return getUsedMemory(StatType.MEAN);
                case 3:  return getUsedMemory(StatType.MEDIAN);
                case 4:  return getUsedMemory(StatType.STD_DEV);
                default: throw new IllegalArgumentException("Unexpected row count");
            }
        });
    }

    /**
     * Resets all values in this timing
     */
    private void reset(int count) {
        this.count = 0;
        Array<String> colKeys = Array.of(label + " (call)", label + " (gc)",  label + " (total)");
        this.frame = DataFrame.ofDoubles(Range.of(0, count).map(String::valueOf), colKeys);
        this.gcTimeStats = StatsCollector.of(new Min(), new Max(), new Mean(), new Median(), new StdDev(true));
        this.callTimeStats = StatsCollector.of(new Min(), new Max(), new Mean(), new Median(), new StdDev(true));
        this.memoryStats = StatsCollector.of(new Min(), new Max(), new Mean(), new Median(), new StdDev(true));
    }

    public DataFrame<String,String> getFrame() {
        return DataFrame.ofDoubles(Array.of("call", "gc"), label).applyDoubles(v -> {
            switch (v.rowOrdinal()) {
                case 0: return callTimeStats.getValue(StatType.MEAN);
                case 1: return gcTimeStats.getValue(StatType.MEAN);
                default:    throw new IllegalArgumentException("");
            }
        });
    }

    /**
     * Executes the runnable the specified number of times and captures timing statistics
     * @param count     the number of times to execute the runnable
     * @param callable  the callable to execute
     * @return          this timing object
     */
    public synchronized PerfStat run(int count, Callable callable) {
        try {
            this.reset(count);
            final int warmUpCount = 2;
            if (memoryEstimator == null) {
                System.out.println("Creating default MemoryEstimator...");
                memoryEstimator = new MemoryEstimator.DefaultMemoryEstimator();
            }
            for (int i=0; i<count + warmUpCount; ++i) {
                this.count++;
                this.beforeEach.forEach(Runnable::run);
                System.gc();
                final long t1 = System.nanoTime();
                final Object result = callable.call();
                final long t2 = System.nanoTime();
                final long memory = result != null ? memoryEstimator.getObjectSize(result) : 0L;
                final long t3 = System.nanoTime();
                if (result != null && result.toString() != null) {
                    System.gc();
                }
                final long t4 = System.nanoTime();
                this.afterEach.forEach(Runnable::run);
                if (i >= warmUpCount) {
                    this.callTimeStats.add(convert(t2 - t1));
                    this.memoryStats.add(memory / Math.pow(1024, 2));
                    this.gcTimeStats.add(convert(t4-t3));
                    this.frame.data().setDouble(i - warmUpCount, 0, convert(t2 - t1));
                    this.frame.data().setDouble(i - warmUpCount, 1, convert(t4 - t3));
                    this.frame.data().setDouble(i - warmUpCount, 2, convert((t2 - t1) + (t4 - t3)));
                }
            }
            return this;
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    /**
     * Returns a rough estimate of currently used memory
     * @return  the used memory for JVM
     */
    private long getUsedMemory() {
        final long total = Runtime.getRuntime().totalMemory();
        final long free = Runtime.getRuntime().freeMemory();
        return total - free;
    }

    /**
     * Runs a timing loop measuring the runnable in milliseconds
     * @param count         the number of times to execute runnable
     * @param callable      the callable to execute
     * @return              the newly created Timing object
     */
    public static PerfStat timeInMillis(int count, Callable callable) {
        return new PerfStat("", TimeUnit.MILLISECONDS).run(count, callable);
    }

    /**
     * Runs a timing loop measuring the runnable in microseconds, and prints results to std out
     * @param description   the description to print to standard out
     * @param count         the number of times to execute runnable
     * @param callable      the callable to execute
     * @return              the newly created Timing object
     */
    public static PerfStat timeInMicros(String description, int count, Callable callable) {
        return new PerfStat("", TimeUnit.MICROSECONDS).run(count, callable).print(description);
    }

    /**
     * Runs a timing loop measuring the runnable in milliseconds, and prints results to std out
     * @param description   the description to print to standard out
     * @param count         the number of times to execute runnable
     * @param callable      the callable to execute
     * @return              the newly created Timing object
     */
    public static PerfStat timeInMillis(String description, int count, Callable callable) {
        return new PerfStat("", TimeUnit.MILLISECONDS).run(count, callable).print(description);
    }


    /**
     * Converts nanoseconds into TimeUnit for this Timing
     * @param nanos     the nanosecond precision time
     * @return          the time measures in the units for this Timing
     */
    private long convert(long nanos) {
        switch (units) {
            case NANOSECONDS:   return nanos;
            case MICROSECONDS:  return nanos / 1000;
            case MILLISECONDS:  return nanos / 1000000;
            case SECONDS:       return nanos / 1000000000;
            case MINUTES:       return (nanos / 1000000000) / 60;
            case HOURS:         return ((nanos / 1000000000) / 60) / 60;
            case DAYS:          return (((nanos / 1000000000) / 60) / 60) / 24;
            default:    throw new IllegalStateException("Unsupported TimeUnits: " + units);
        }
    }

    /**
     * Returns the name for units
     * @return  the unit name
     */
    private String getUnitName() {
        switch (getUnits()) {
            case NANOSECONDS:   return "nanos";
            case MICROSECONDS:  return "micros";
            case MILLISECONDS:  return "millis";
            case SECONDS:       return "secs";
            case MINUTES:       return "mins";
            case HOURS:         return "hours";
            case DAYS:          return "days";
            default:    throw new IllegalStateException("Unsupported TimeUnits: " + units);
        }
    }

    /**
     * Function to time a task and return results as a DataFrame
     * @param count     the number of times to run the task
     * @param units     the timing units
     * @param callable  the callable to time
     * @return          the DataFrame of results
     */
    public static PerfStat run(String label, int count, TimeUnit units, Callable callable) {
        return new PerfStat(label, units).run(count, callable);
    }

    /**
     * Function to run multiple tasks sequentially and compile a DataFrame with all the results
     * @param count     the number of times to run each task
     * @param units     the timing units
     * @param includeGc if true, include GC times in the results
     * @param consumer  the consumer user to populate a apply with runnables
     * @return          the DataFrame of results
     */
    public static DataFrame<String,String> run(int count, TimeUnit units, boolean includeGc, Consumer<Tasks> consumer) {
        final Tasks tasks = new Tasks();
        consumer.accept(tasks);
        final List<DataFrame<String,String>> resultList = new ArrayList<>();
        for (String key : tasks.taskMap.keySet()) {
            final Callable<?> callable = tasks.taskMap.get(key);
            final PerfStat perStat = new PerfStat(key, units);
            perStat.beforeEach.addAll(tasks.beforeEach);
            perStat.afterEach.addAll(tasks.afterEach);
            perStat.run(count, callable);
            final DataFrame<String,String> timeStats = perStat.getRunStats();
            if (!includeGc) {
                resultList.add(timeStats);
            } else {
                final DataFrame<String,String> gcStats = perStat.getGcStats();
                resultList.add(timeStats.applyDoubles(v -> {
                    final double runTime = v.getDouble();
                    final double gcTime = gcStats.data().getDouble(v.rowKey(), v.colKey());
                    return runTime + gcTime;
                }));
            }
        }
        return DataFrame.union(resultList);
    }


    /**
     * Prints this Timing to standard out
     * @return  this timing object
     */
    public PerfStat print() {
        System.out.println(this);
        return this;
    }

    /**
     * Prints this Timing to standard out
     * @return  this timing object
     */
    public PerfStat print(String description) {
        System.out.println(toString(description));
        return this;
    }

    /**
     * Returns a string representation of this object
     * @param description   the description for timing
     * @return              the string representation
     */
    public String toString(String description) {
        final String label = getUnitName();
        final StringBuilder text = new StringBuilder();
        text.append("Timing");
        if (description != null) {
            text.append("[");
            text.append(description);
            text.append("]");
        }
        return text.toString();
    }

    @Override()
    public String toString() {
        return toString(null);
    }


    /**
     * An object to capture batch tasks to run in sequence
     */
    public static class Tasks {

        List<Runnable> beforeEach = new ArrayList<>();
        List<Runnable> afterEach = new ArrayList<>();
        Map<String,Callable<?>> taskMap = new LinkedHashMap<>();

        /**
         * Adds a runnable to be run before each callable, but not timed
         * @param runnable  the runnable to run before each task
         * @return          this Tasks interface
         */
        public Tasks beforeEach(Runnable runnable) {
            this.beforeEach.add(runnable);
            return this;
        }

        /**
         * Adds a runnable to be run after each callable, but not timed
         * @param runnable  the runnable to be run after each task
         * @return          this Tasks interface
         */
        public Tasks afterEach(Runnable runnable) {
            this.afterEach.add(runnable);
            return this;
        }

        /**
         * Puts a callable in this tasks definition
         * @param label     the label for task
         * @param task      the task callable
         * @return          this Tasks interface
         */
        public Tasks put(String label, Callable<?> task) {
            this.taskMap.put(label, task);
            return this;
        }

    }
}
