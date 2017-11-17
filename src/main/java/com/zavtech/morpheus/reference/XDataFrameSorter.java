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
package com.zavtech.morpheus.reference;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameColumn;
import com.zavtech.morpheus.frame.DataFrameRow;
import com.zavtech.morpheus.range.Range;

/**
 * A class that is designed to sort a DataFrame in either the row or column dimension
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @see <a href="http://mechanical-sympathy.blogspot.com/2012/08/memory-access-patterns-are-important.html">Mechanical Sympathy</a>
 *
 * @author  Xavier Witdouck
 */
class XDataFrameSorter {


    /**
     * Sorts the rows of a DataFrame according to data in the specified column
     * @param frame         the frame to sort
     * @param colKey        the column key to sort by
     * @param ascending     true for ascending, false for descending
     * @param parallel      true for parallel sort
     * @return              the sorted DataFrame
     */
    static <R,C> XDataFrame<R,C> sortRows(XDataFrame<R,C> frame, C colKey, boolean ascending, boolean parallel) {
        return sortRows(frame, Collections.singletonList(colKey), ascending, parallel);
    }


    /**
     * Sorts the column of a DataFrame according to data in the specified rows
     * @param frame         the frame to sort
     * @param rowKey        the row key to sort by
     * @param ascending     true for ascending, false for descending
     * @param parallel      true for parallel sort
     * @return              the sorted DataFrame
     */
    static <R,C> XDataFrame<R,C> sortCols(XDataFrame<R,C> frame, R rowKey, boolean ascending, boolean parallel) {
        return sortCols(frame, Collections.singletonList(rowKey), ascending, parallel);
    }


    /**
     * Sorts the rows of a DataFrame according to the row keys
     * @param frame         the frame to sort
     * @param ascending     true for ascending, false for descending
     * @param parallel      true for parallel sort
     * @return              the sorted DataFrame
     */
    static <R,C> XDataFrame<R,C> sortRows(XDataFrame<R,C> frame, boolean ascending, boolean parallel) {
        frame.rowKeys().sort(parallel, ascending);
        return frame;
    }


    /**
     * Sorts the column of a DataFrame according to the column keys
     * @param frame         the frame to sort
     * @param ascending     true for ascending, false for descending
     * @param parallel      true for parallel sort
     * @return              the sorted DataFrame
     */
    static <R,C> XDataFrame<R,C> sortCols(XDataFrame<R,C> frame, boolean ascending, boolean parallel) {
        frame.colKeys().sort(parallel, ascending);
        return frame;
    }


    /**
     * Sorts the rows of a DataFrame according to data in the specified columns
     * @param frame         the frame to sort
     * @param colKeys       the column keys to sort by, in order of precedence
     * @param ascending     true for ascending, false for descending
     * @param parallel      true for parallel sort
     * @return              the sorted DataFrame
     */
    static <R,C> XDataFrame<R,C> sortRows(XDataFrame<R,C> frame, List<C> colKeys, boolean ascending, boolean parallel) {
        final int multiplier = ascending ? 1 : -1;
        final XDataFrameContent<R,C> content = frame.content();
        final XDataFrameComparator comparator = content.createRowComparator(colKeys, multiplier);
        frame.rowKeys().sort(parallel, comparator);
        return frame;
    }


    /**
     * Sorts the column of a DataFrame according to data in the specified rows
     * @param frame         the frame to sort
     * @param rowKeys       the row keys to sort by, in order of precedence
     * @param ascending     true for ascending, false for descending
     * @param parallel      true for parallel sort
     * @return              the sorted DataFrame
     */
    static <R,C> XDataFrame<R,C> sortCols(XDataFrame<R,C> frame, List<R> rowKeys, boolean ascending, boolean parallel) {
        final int multiplier = ascending ? 1 : -1;
        final XDataFrameContent<R,C> content = frame.content();
        final XDataFrameComparator comparator = content.createColComparator(rowKeys, multiplier);
        frame.colKeys().sort(parallel, comparator);
        return frame;
    }


    /**
     * Sorts rows of the DataFrame based on the user provided comparator
     * @param frame         the frame reference
     * @param parallel      true for parallel sort
     * @param comparator    the user provided comparator
     * @return              the sorted DataFrame
     */
    static <R,C> XDataFrame<R,C> sortRows(XDataFrame<R,C> frame, boolean parallel, Comparator<DataFrameRow<R,C>> comparator) {
        if (comparator == null) {
            frame.rowKeys().resetOrder();
            return frame;
        } else {
            final XDataFrameComparator rowComparator = XDataFrameComparator.createRowComparator(frame, comparator);
            frame.rowKeys().sort(parallel, rowComparator);
            return frame;
        }
    }


    /**
     * Sorts rows of the DataFrame based on the user provided comparator
     * @param frame         the frame reference
     * @param parallel      true for parallel sort
     * @param comparator    the user provided comparator
     * @return              the sorted DataFrame
     */
    static <R,C> XDataFrame<R,C> sortCols(XDataFrame<R,C> frame, boolean parallel, Comparator<DataFrameColumn<R,C>> comparator) {
        if (comparator == null) {
            frame.colKeys().sort(parallel, null);
            return frame;
        } else {
            final XDataFrameComparator colComparator = XDataFrameComparator.createColComparator(frame, comparator);
            frame.colKeys().sort(parallel, colComparator);
            return frame;
        }
    }


    public static void main(String[] args) {
        final LocalDateTime start = LocalDateTime.now();
        final Array<LocalDateTime> rowKeys = Range.of(start, start.plusSeconds(10000000), Duration.ofSeconds(1)).toArray().shuffle(1);
        final Array<String> colKeys = Array.of("A", "B", "C", "D");
        final DataFrame<LocalDateTime,String> frame = DataFrame.ofDoubles(rowKeys, colKeys).applyDoubles(v -> Math.random());
        for (int i=0; i<20; ++i) {
            frame.applyDoubles(v -> Math.random());
            final MyComparator comp = new MyComparator();
            final long t1 = System.nanoTime();
            //frame.rows().parallel().sort(true);
            frame.rows().parallel().sort(true);
            //frame.rows().parallel().sort(true, "A");
            final long t2 = System.nanoTime();
            System.out.println("Sorted DataFrame in " + ((t2-t1)/1000000) + " millis");
        }
    }


    private static class MyComparator implements Comparator<DataFrameRow<LocalDateTime,String>> {
        @Override
        public final int compare(DataFrameRow<LocalDateTime,String> row1, DataFrameRow<LocalDateTime,String> row2) {
            final double v1 = row1.getDouble(0);
            final double v2 = row2.getDouble(0);
            return Double.compare(v1, v2);
        }
    }

}
