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

import java.util.Comparator;
import java.util.stream.IntStream;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrameColumn;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameRow;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.util.IntComparator;

/**
 * An IntComparator that is used to sort a DataFrame in either the row or column dimension
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @see <a href="http://mechanical-sympathy.blogspot.com/2012/08/memory-access-patterns-are-important.html">Mechanical Sympathy</a>
 *
 * @author  Xavier Witdouck
 */
abstract class XDataFrameComparator implements IntComparator {

    protected Index<?> index;

    /**
     * Constructor
     */
    private XDataFrameComparator() {
        super();
    }

    /**
     * Returns a composite comparator based on the array of comparators provided
     * @param comparators   the array of comparators from which to create a composite
     * @return              the composite comparator
     */
    static XDataFrameComparator create(XDataFrameComparator... comparators) {
        if (comparators.length == 1) {
            return comparators[0];
        } else {
            return new CompositeComparator(comparators);
        }
    }

    /**
     * Returns a newly created comparator to sort rows according to a user provided row comparator
     * @param frame         the frame reference
     * @param comparator    the user provided comparator to wrap
     * @param <R>           the row key type
     * @param <C>           the column key type
     * @return              the newly created comparator
     */
    static <R,C> XDataFrameComparator createRowComparator(XDataFrame<R,C> frame, Comparator<DataFrameRow<R,C>> comparator) {
        return new RowVectorComparator<>(frame, comparator);
    }

    /**
     * Returns a newly created comparator to sort columns according to a user provided column comparator
     * @param frame         the frame reference
     * @param comparator    the user provided comparator to wrap
     * @param <R>           the row key type
     * @param <C>           the column key type
     * @return              the newly created comparator
     */
    static <R,C> XDataFrameComparator createColComparator(XDataFrame<R,C> frame, Comparator<DataFrameColumn<R,C>> comparator) {
        return new ColVectorComparator<>(frame, comparator);
    }


    /**
     * Returns a newly created comparator to sort the array specified
     * @param array         the array to sort
     * @param multiplier    the multiplier for ascending / descending
     * @return              the newly created comparator
     */
    static XDataFrameComparator create(Array<?> array, int multiplier) {
        switch (array.typeCode()) {
            case BOOLEAN:           return createBooleanComparator(array, multiplier);
            case INTEGER:           return createIntegerComparator(array, multiplier);
            case LONG:              return createLongComparator(array, multiplier);
            case DOUBLE:            return createDoubleComparator(array, multiplier);
            case DATE:              return createLongComparator(array, multiplier);
            case INSTANT:           return createLongComparator(array, multiplier);
            case LOCAL_DATE:        return createLongComparator(array, multiplier);
            case LOCAL_TIME:        return createLongComparator(array, multiplier);
            case LOCAL_DATETIME:    return createLongComparator(array, multiplier);
            case ZONED_DATETIME:    return createLongComparator(array, multiplier);
            default:                return createValueComparator(array, multiplier);
        }
    }

    /**
     * Sets the index that will be sorted with this comparator
     * @param index     the index that will be affected by the sort
     * @return          this comparator
     */
    XDataFrameComparator withIndex(Index<?> index) {
        this.index = index;
        return this;
    }

    @Override
    public int compare(int ordinal1, int ordinal2) {
        final int index1 = index.getIndexForOrdinal(ordinal1);
        final int index2 = index.getIndexForOrdinal(ordinal2);
        return compareValues(index1, index2);
    }

    /**
     * Returns -1, 0, 1 if the first argument is less than, equal to, or greater than the second
     * @param index1    the index to the first argument
     * @param index2    the index to the second argument
     * @return          -1, 0, 1 if the first argument is less than, equal to, or greater than the second
     */
    abstract int compareValues(int index1, int index2);


    /**
     * Returns a newly created comparator to sort the array specified
     * @param array         the array to sort
     * @param multiplier    the multiplier for ascending / descending
     * @return              the newly created comparator
     */
    private static XDataFrameComparator createBooleanComparator(Array<?> array, int multiplier) {
        return new XDataFrameComparator() {
            @Override
            final int compareValues(int index1, int index2) {
                final boolean v1 = array.getBoolean(index1);
                final boolean v2 = array.getBoolean(index2);
                return multiplier * Boolean.compare(v1, v2);
            }
        };
    }


    /**
     * Returns a newly created comparator to sort the array specified
     * @param array         the array to sort
     * @param multiplier    the multiplier for ascending / descending
     * @return              the newly created comparator
     */
    private static XDataFrameComparator createIntegerComparator(Array<?> array, int multiplier) {
        return new XDataFrameComparator() {
            @Override
            final int compareValues(int index1, int index2) {
                final int v1 = array.getInt(index1);
                final int v2 = array.getInt(index2);
                return multiplier * Integer.compare(v1, v2);
            }
        };
    }


    /**
     * Returns a newly created comparator to sort the array specified
     * @param array         the array to sort
     * @param multiplier    the multiplier for ascending / descending
     * @return              the newly created comparator
     */
    private static XDataFrameComparator createLongComparator(Array<?> array, int multiplier) {
        return new XDataFrameComparator() {
            @Override
            final int compareValues(int index1, int index2) {
                final long v1 = array.getLong(index1);
                final long v2 = array.getLong(index2);
                return multiplier * Long.compare(v1, v2);
            }
        };
    }


    /**
     * Returns a newly created comparator to sort the array specified
     * @param array         the array to sort
     * @param multiplier    the multiplier for ascending / descending
     * @return              the newly created comparator
     */
    private static XDataFrameComparator createDoubleComparator(Array<?> array, int multiplier) {
        return new XDataFrameComparator() {
            @Override
            final int compareValues(int index1, int index2) {
                final double v1 = array.getDouble(index1);
                final double v2 = array.getDouble(index2);
                return multiplier * Double.compare(v1, v2);
            }
        };
    }


    /**
     * Returns a newly created comparator to sort the array specified
     * @param array         the array to sort
     * @param multiplier    the multiplier for ascending / descending
     * @return              the newly created comparator
     */
    private static XDataFrameComparator createValueComparator(Array<?> array, int multiplier) {
        return new XDataFrameComparator() {
            @Override
            @SuppressWarnings("unchecked")
            final int compareValues(int index1, int index2) {
                final Comparable v1 = (Comparable)array.getValue(index1);
                final Comparable v2 = (Comparable)array.getValue(index2);
                if (v1 != null && v2 != null) {
                    return multiplier * v1.compareTo(v2);
                } else if (v1 == null && v2 == null) {
                    return 0;
                } else if (v1 == null) {
                    return -1 * multiplier;
                } else {
                    return multiplier;
                }
            }
        };
    }



    /**
     * A composite comparator that combines one or more comparators.
     */
    private static class CompositeComparator extends XDataFrameComparator {

        private int count;
        private XDataFrameComparator[] comparators;

        /**
         * Constructor
         * @param comparators   the child comparators
         */
        private CompositeComparator(XDataFrameComparator[] comparators) {
            this.count = comparators.length;
            this.comparators = comparators;
        }

        @Override
        public IntComparator copy() {
            try {
                final CompositeComparator clone = (CompositeComparator)super.clone();
                clone.count = count;
                clone.index = index;
                clone.comparators = new XDataFrameComparator[count];
                for (int i=0; i<count; ++i) {
                    clone.comparators[i] = (XDataFrameComparator)comparators[i].copy();
                }
                return clone;
            } catch (Exception ex) {
                throw new DataFrameException("Failed to clone comparator", ex);
            }
        }

        @Override
        int compareValues(int index1, int index2) {
            throw new UnsupportedOperationException("This comparator only supports view based indexes");
        }

        @Override
        public final int compare(int ordinal1, int ordinal2) {
            final int index1 = index.getIndexForOrdinal(ordinal1);
            final int index2 = index.getIndexForOrdinal(ordinal2);
            for (int i=0; i<count; ++i) {
                final int result = comparators[i].compareValues(index1, index2);
                if (result != 0) {
                    return result;
                }
            }
            return 0;
        }
    }


    /**
     * An IntComparator that wraps a user provided comparator to sort row vectors of a DataFrame
     */
    private static class RowVectorComparator<R,C> extends XDataFrameComparator {

        private XDataFrame<R,C> frame;
        private XDataFrameRow<R,C> row1;
        private XDataFrameRow<R,C> row2;
        private Comparator<DataFrameRow<R,C>> comparator;

        /**
         * Constructor
         * @param frame         the DataFrame reference
         * @param comparator    the user provided comparator
         */
        private RowVectorComparator(XDataFrame<R,C> frame, Comparator<DataFrameRow<R,C>> comparator) {
            this.frame = frame;
            this.comparator = comparator;
            this.row1 = new XDataFrameRow<>(frame, false);
            this.row2 = new XDataFrameRow<>(frame, false);
        }

        @Override()
        public final int compare(int ordinal1, int ordinal2) {
            this.row1.moveTo(ordinal1);
            this.row2.moveTo(ordinal2);
            return comparator.compare(row1, row2);
        }

        @Override
        int compareValues(int index1, int index2) {
            throw new UnsupportedOperationException("This comparator only supports view based indexes");
        }

        @Override()
        @SuppressWarnings("unchecked")
        public final IntComparator copy() {
            try {
                final RowVectorComparator clone = (RowVectorComparator)super.clone();
                clone.frame = this.frame;
                clone.index = this.index;
                clone.comparator = this.comparator;
                clone.row1 = new XDataFrameRow<>(frame, false);
                clone.row2 = new XDataFrameRow<>(frame, false);
                return clone;
            } catch (CloneNotSupportedException ex) {
                throw new DataFrameException("Failed to clone row comparator for parallel sort", ex);
            }
        }
    }



    /**
     * An IntComparator that wraps a user provided comparator to sort column vectors of a DataFrame
     */
    private static class ColVectorComparator<R,C> extends XDataFrameComparator {

        private XDataFrame<R,C> frame;
        private XDataFrameColumn<R,C> column1;
        private XDataFrameColumn<R,C> column2;
        private Comparator<DataFrameColumn<R,C>> comparator;

        /**
         * Constructor
         * @param frame         the DataFrame reference
         * @param comparator    the user provided comparator
         */
        private ColVectorComparator(XDataFrame<R,C> frame, Comparator<DataFrameColumn<R,C>> comparator) {
            this.frame = frame;
            this.comparator = comparator;
            this.column1 = new XDataFrameColumn<>(frame, false);
            this.column2 = new XDataFrameColumn<>(frame, false);
        }

        @Override()
        public final int compare(int ordinal1, int ordinal2) {
            this.column1.moveTo(ordinal1);
            this.column2.moveTo(ordinal2);
            return comparator.compare(column1, column2);
        }

        @Override
        int compareValues(int index1, int index2) {
            throw new UnsupportedOperationException("This comparator only supports view based indexes");
        }

        @Override()
        @SuppressWarnings("unchecked")
        public final IntComparator copy() {
            try {
                final ColVectorComparator clone = (ColVectorComparator)super.clone();
                clone.frame = this.frame;
                clone.index = this.index;
                clone.comparator = this.comparator;
                clone.column1 = new XDataFrameColumn<>(frame, false);
                clone.column2 = new XDataFrameColumn<>(frame, false);
                return clone;
            } catch (CloneNotSupportedException ex) {
                throw new DataFrameException("Failed to clone row comparator for parallel sort", ex);
            }
        }
    }



    public static void main(String[] args) {
        final int size = 5000000;
        IntStream.range(0, 5).forEach(k -> {
            final Array<Double> values = Array.of(Double.class, size).applyDoubles(v -> Math.random());
            final Index<Double> index = Index.of(values);
            final XDataFrameComparator comparator = XDataFrameComparator.create(values, 1).withIndex(index);
            final long t1 = System.currentTimeMillis();
            index.sort(true, comparator);
            final long t2 = System.currentTimeMillis();
            System.out.println("Sorted array in " + (t2-t1) + " millis");
            for (int i=1; i<index.size(); ++i) {
                final double v1 = index.getKey(i-1);
                final double v2 = index.getKey(i);
                final int comp = Double.compare(v1, v2);
                if (comp > 0) {
                    throw new RuntimeException("The indexes are not sorted for the array");
                }
            }
        });
    }


}
