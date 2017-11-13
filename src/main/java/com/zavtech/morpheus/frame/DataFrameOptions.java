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
package com.zavtech.morpheus.frame;

import java.util.concurrent.Callable;
import java.util.function.ToIntFunction;

/**
 * An enum which exposes various options for controlling the behaviour of certain matrix functions.
 *
 * <h3>Overview</h3>
 * <p>
 * This class provides a mechanism to control the behaviour of certain algorithms supported by the
 * Morpheus API.  In all cases options will default to some appropriate setting, however if a specific
 * behaviour is desired an option can be changed on this class.  This design allows new options to
 * be added in time without requiring a change to any method signatures, which will enhance backward
 * compatibility.
 * </p>
 *
 * <p>
 * An example use case would be to modify the way the ranking algorithm works which ranks a sequence
 * of numbers according to their natural order. The behaviour of the algorithm can be changed with
 * respect to how it deals with NaN values in the sequence and also matching values.  The code fragment
 * below demonstrates how this can be done:
 * <pre>
 *      //Modify options for this thread
 *      DataFrameOptions.setNanStrategy(DataFrameOptions.MINIMUM);
 *      DataFrameOptions.setTieStrategy(DataFrameOptions.AVERAGE);
 *
 *      //Now call rankRows()
 *      DataFrame&lt;String,String&gt; ranks = matrix.rankRows();
 * </pre>
 * It should be noted that changing options apply to the currently executing thread so they are not
 * globally configurable.
 * </p>
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public enum DataFrameOptions {

    MINIMUM,
    MAXIMUM,
    AVERAGE;

    private static final int processorCount = Runtime.getRuntime().availableProcessors();

    private static ToIntFunction<DataFrame<?,?>> defaultRowSplitThreshold;
    private static ToIntFunction<DataFrame<?,?>> defaultColSplitThreshold;
    private static final ThreadLocal<DataFrameOptions> nanStrategy = new ThreadLocal<>();
    private static final ThreadLocal<DataFrameOptions> tieStrategy = new ThreadLocal<>();
    private static final ThreadLocal<ToIntFunction<DataFrame<?,?>>> rowSplitThreshold = new ThreadLocal<>();
    private static final ThreadLocal<ToIntFunction<DataFrame<?,?>>> colSplitThreshold = new ThreadLocal<>();

    private static final ThreadLocal<Boolean> ignoreDuplicates = new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
            return Boolean.TRUE;
        }
    };

    /**
     * Static initializer
     */
    static {
        DataFrameOptions.setDefaultRowSplitThreshold(frame -> {
            final int rowCount = frame.rows().count();
            final int groupCount = rowCount / processorCount;
            return Math.max(groupCount, 1);
        });

        DataFrameOptions.setDefaultColSplitThreshold(frame -> {
            final int colCount = frame.cols().count();
            final int groupCount = colCount / processorCount;
            return Math.max(groupCount, 1);
        });
    }

    /**
     * Returns the NaN strategy for the current thread ranking algorithm
     * @return      the NaN strategy for current thread ranking algorithm
     */
    public static DataFrameOptions getNanStrategy() {
        final DataFrameOptions result = nanStrategy.get();
        return result != null ? result : DataFrameOptions.MINIMUM;
    }

    /**
     * Returns the tie strategy for the current thread ranking algorithm
     * @return      the tie strategy for current thread ranking algorithm
     */
    public static DataFrameOptions getTieStrategy() {
        final DataFrameOptions result = tieStrategy.get();
        return result != null ? result : DataFrameOptions.AVERAGE;
    }

    /**
     * Returns the threshold above which job splitting should occur in the row dimension for enhance parallel execution
     * @return      the row count threshold above which splitting should occur
     */
    public static int getRowSplitThreshold(DataFrame<?,?> frame) {
        final ToIntFunction<DataFrame<?,?>> function = rowSplitThreshold.get();
        return function != null ? function.applyAsInt(frame) : defaultRowSplitThreshold.applyAsInt(frame);
    }

    /**
     * Returns the threshold above which job splitting should occur in the column dimension for enhance parallel execution
     * @return      the column count threshold above which splitting should occur
     */
    public static int getColumnSplitThreshold(DataFrame<?,?> frame) {
        final ToIntFunction<DataFrame<?,?>> function = colSplitThreshold.get();
        return function != null ? function.applyAsInt(frame) : defaultColSplitThreshold.applyAsInt(frame);
    }

    /**
     * Sets the NaN strategy to use for the current thread ranking algorithm
     * @param strategy  the NaN strategy (MINIMUM | MAXIMUM)
     * @throws DataFrameException  if an unsupported strategy is specified
     */
    public static void setNanStrategy(DataFrameOptions strategy) throws DataFrameException {
        switch (strategy) {
            case MINIMUM:   nanStrategy.set(strategy);  break;
            case MAXIMUM:   nanStrategy.set(strategy);  break;
            default:    throw new DataFrameException("Unsupported NaN strategy specified: " + strategy);
        }
    }

    /**
     * Sets the tie strategy to use for the current thread ranking algorithm
     * @param strategy  the tie strategy (MINIMUM | MAXIMUM | AVERAGE).
     * @throws DataFrameException  if an unsupported strategy is specified
     */
    public static void setTieStrategy(DataFrameOptions strategy) throws DataFrameException {
        switch (strategy) {
            case MINIMUM:       tieStrategy.set(strategy);  break;
            case MAXIMUM:       tieStrategy.set(strategy);  break;
            case AVERAGE:   tieStrategy.set(strategy);  break;
            default:    throw new DataFrameException("Unsupported tie strategy specified: " + strategy);
        }
    }

    /**
     * Sets the function that defines the default splitting threshold in row dimension
     * @param defaultRowSplitThreshold  the default function
     */
    public static void setDefaultRowSplitThreshold(ToIntFunction<DataFrame<?,?>> defaultRowSplitThreshold) {
        DataFrameOptions.defaultRowSplitThreshold = defaultRowSplitThreshold;
    }

    /**
     * Sets the function that defines the default splitting threshold in column dimension
     * @param defaultColSplitThreshold  the default function
     */
    public static void setDefaultColSplitThreshold(ToIntFunction<DataFrame<?,?>> defaultColSplitThreshold) {
        DataFrameOptions.defaultColSplitThreshold = defaultColSplitThreshold;
    }

    /**
     * Returns true if operations on the current thread should ignore duplicates
     * @return      true if operations on the current thread should ignore duplicates
     */
    public static boolean isIgnoreDuplicates() {
        return ignoreDuplicates.get();
    }


    public static void whileIgnoringDuplicates(Runnable runnable) {
        final boolean initial = ignoreDuplicates.get();
        ignoreDuplicates.set(true);
        try {
            runnable.run();
        } finally {
            ignoreDuplicates.set(initial);
        }
    }

    public static void whileNotIgnoringDuplicates(Runnable runnable) {
        final boolean initial = ignoreDuplicates.get();
        ignoreDuplicates.set(false);
        try {
            runnable.run();
        } finally {
            ignoreDuplicates.set(initial);
        }
    }

    public static <T> T whileIgnoringDuplicates(Callable<T> callable) {
        final boolean initial = ignoreDuplicates.get();
        ignoreDuplicates.set(true);
        try {
            return callable.call();
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        } finally {
            ignoreDuplicates.set(initial);
        }
    }

    public static <T> T whileNotIgnoringDuplicates(Callable<T> callable) {
        final boolean initial = ignoreDuplicates.get();
        ignoreDuplicates.set(false);
        try {
            return callable.call();
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        } finally {
            ignoreDuplicates.set(initial);
        }
    }

}
