/**
 * Copyright (C) 2002-2014 Sebastiano Vigna
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

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

/**
 * This is a standard interface to a sorting algorithm that works off an IntComparator and a Swapper.
 *
 * The inspiration for this class comes entirely from FastUtil, so all credit to Sebastiano Vigna.
 *
 * @see <a href="http://vigna.di.unimi.it/">Sebastiano Vigna</a>
 * @see <a href="http://fastutil.di.unimi.it/">Fastutil</a>
 */
public abstract class SortAlgorithm {

    public enum Type { FAST_UTIL }

    private static Type type = Type.FAST_UTIL;

    /**
     * Returns a parallel version of this sorting algorithm
     * @return  a parallel version of this algorithm
     */
    public abstract SortAlgorithm parallel();

    /**
     * Returns a sequential version of this sorting alforithm
     * @return  a sequantial version of this algorithm
     */
    public abstract SortAlgorithm sequential();

    /**
     * Sorts a dataset hidden behind the comparator & swapper between the indexes specified
     * @param from          the from index for sorting range, inclusive
     * @param to            the to index for sorting range, exclusive
     * @param comparator    the comparator to interrogate for relative positions
     * @param swapper       the swapper used to swap positions of elements in the data structure
     */
    public abstract void sort(int from, int to, IntComparator comparator, Swapper swapper);


    /**
     * Sets the default algorithm for this process
     * @param type  the algorithm type
     */
    public static void setDefault(Type type) {
        if (type == null) {
            throw new IllegalArgumentException("The sorting type cannot be null");
        } else {
            SortAlgorithm.type = type;
        }
    }


    /**
     * Returns a reference to the default Sorting algorithm
     * @param parallel  true for the parallel version
     * @return      the default sorting algorithm
     */
    public static SortAlgorithm getDefault(boolean parallel) {
        switch (type) {
            case FAST_UTIL:     return fastUtil(parallel);
            default:            throw new IllegalStateException("Unsupported sorting type: " + type);
        }
    }

    /**
     * Returns the sorting algorithm for the args specified
     * @param type      the algorithm type
     * @param parallel  true for parallel version, false for sequential
     * @return          the sorting algorithm
     */
    public static SortAlgorithm of(Type type, boolean parallel) {
        switch (type) {
            case FAST_UTIL:     return fastUtil(parallel);
            default:            throw new IllegalStateException("Unsupported sorting type: " + type);
        }
    }

    /**
     * Returns the FastUtil implementation of QuickSort
     * @param parallel  true for the parallel version
     * @return  FastUtil implementation of QuickSort
     */
    public static SortAlgorithm fastUtil(boolean parallel) {
        return parallel ? new FastUtilParallel() : new FastUtilSequential();
    }

    /**
     * Returns the index of the median of the three indexed chars.
     */
    private static int med3( final int a, final int b, final int c, final IntComparator comp ) {
        int ab = comp.compare( a, b );
        int ac = comp.compare( a, c );
        int bc = comp.compare( b, c );
        return ( ab < 0 ? ( bc < 0 ? b : ac < 0 ? c : a ) :  ( bc > 0 ? b : ac > 0 ? c : a ) );
    }

    /**
     * Swaps two sequences of elements using a provided swapper.
     * @param swapper the swapper.
     * @param a a position in {@code x}.
     * @param b another position in {@code x}.
     * @param n the number of elements to exchange starting at {@code a} and {@code b}.
     */
    private static void swap( final Swapper swapper, int a, int b, final int n ) {
        for ( int i = 0; i < n; i++, a++, b++ ) {
            swapper.swap( a, b );
        }
    }



    /**
     * @author Sebastiano Vigna
     * @see <a href="http://vigna.di.unimi.it/">Sebastiano Vigna</a>
     * @see <a href="http://fastutil.di.unimi.it/">Fastutil</a>
     */
    private static class FastUtilSequential extends SortAlgorithm {

        private static final int QUICKSORT_NO_REC = 16;
        private static final int QUICKSORT_MEDIAN_OF_9 = 128;


        @Override
        public SortAlgorithm parallel() {
            return new FastUtilParallel();
        }

        @Override
        public SortAlgorithm sequential() {
            return this;
        }

        /**
         * Sorts the specified range of elements using the specified swapper and according to the order induced by the specified
         * comparator using parallel quicksort.
         *
         * <p>The sorting algorithm is a tuned quicksort adapted from Jon L. Bentley and M. Douglas
         * McIlroy, &ldquo;Engineering a Sort Function&rdquo;, <i>Software: Practice and Experience</i>, 23(11), pages
         * 1249&minus;1265, 1993.
         *
         * @param from the index of the first element (inclusive) to be sorted.
         * @param to the index of the last element (exclusive) to be sorted.
         * @param comp the comparator to determine the order of the generic data.
         * @param swapper an object that knows how to swap the elements at any two positions.
         */
        public void sort( final int from, final int to, final IntComparator comp, final Swapper swapper ) {
            final int len = to - from;
            // Insertion sort on smallest arrays
            if ( len < QUICKSORT_NO_REC ) {
                for ( int i = from; i < to; i++ )
                    for ( int j = i; j > from && ( comp.compare( j - 1, j ) > 0 ); j-- ) {
                        swapper.swap( j, j - 1 );
                    }
                return;
            }

            // Choose a partition element, v
            int m = from + len / 2; // Small arrays, middle element
            int l = from;
            int n = to - 1;
            if ( len > QUICKSORT_MEDIAN_OF_9 ) { // Big arrays, pseudomedian of 9
                int s = len / 8;
                l = med3( l, l + s, l + 2 * s, comp );
                m = med3( m - s, m, m + s, comp );
                n = med3( n - 2 * s, n - s, n, comp );
            }
            m = med3( l, m, n, comp ); // Mid-size, med of 3
            // int v = x[m];

            int a = from;
            int b = a;
            int c = to - 1;
            // Establish Invariant: v* (<v)* (>v)* v*
            int d = c;
            while ( true ) {
                int comparison;
                while ( b <= c && ( ( comparison = comp.compare( b, m ) ) <= 0 ) ) {
                    if ( comparison == 0 ) {
                        // Fix reference to pivot if necessary
                        if ( a == m ) m = b;
                        else if ( b == m ) m = a;
                        swapper.swap( a++, b );
                    }
                    b++;
                }
                while ( c >= b && ( ( comparison = comp.compare( c, m ) ) >= 0 ) ) {
                    if ( comparison == 0 ) {
                        // Fix reference to pivot if necessary
                        if ( c == m ) m = d;
                        else if ( d == m ) m = c;
                        swapper.swap( c, d-- );
                    }
                    c--;
                }
                if ( b > c ) break;
                // Fix reference to pivot if necessary
                if ( b == m ) m = d;
                else if ( c == m ) m = c;
                swapper.swap( b++, c-- );
            }

            // Swap partition elements back to middle
            int s;
            s = Math.min( a - from, b - a );
            swap( swapper, from, b - s, s );
            s = Math.min( d - c, to - d - 1 );
            swap( swapper, b, to - s, s );

            // Recursively sort non-partition-elements
            if ( ( s = b - a ) > 1 ) sort( from, from + s, comp, swapper );
            if ( ( s = d - c ) > 1 ) sort( to - s, to, comp, swapper );
        }
    }


    /**
     * A Sorting implementation that dispatches Sebastiano's FJ Parallel Quick Sort task
     * @author Xavier Witdouck
     */
    private static class FastUtilParallel extends SortAlgorithm {

        @Override
        public SortAlgorithm parallel() {
            return this;
        }

        @Override
        public SortAlgorithm sequential() {
            return new FastUtilSequential();
        }

        @Override
        public void sort(int from, int to, IntComparator comp, Swapper swapper) {
            ForkJoinPool.commonPool().invoke(new FastUtilForkJoinQuickSort(from, to, comp, swapper));
        }
    }


    /**
     * @author Sebastiano Vigna
     * @see <a href="http://vigna.di.unimi.it/">Sebastiano Vigna</a>
     * @see <a href="http://fastutil.di.unimi.it/">Fastutil</a>
     */
    private class FastUtilForkJoinQuickSort extends RecursiveAction {

        private static final int PARALLEL_QUICKSORT_NO_FORK = 8192;

        private final int from;
        private final int to;
        private final IntComparator comp;
        private final Swapper swapper;

        private FastUtilForkJoinQuickSort(final int from, final int to, final IntComparator comp, final Swapper swapper ) {
            this.from = from;
            this.to = to;
            this.comp = comp.copy();
            this.swapper = swapper;
        }

        @Override
        protected void compute() {
            final int len = to - from;
            if ( len < PARALLEL_QUICKSORT_NO_FORK ) {
                SortAlgorithm.fastUtil(false).sort( from, to, comp, swapper );
                return;
            }
            // Choose a partition element, v
            int m = from + len / 2;
            int l = from;
            int n = to - 1;
            int s = len / 8;
            l = med3( l, l + s, l + 2 * s, comp );
            m = med3( m - s, m, m + s, comp );
            n = med3( n - 2 * s, n - s, n, comp );
            m = med3( l, m, n, comp );
            // Establish Invariant: v* (<v)* (>v)* v*
            int a = from, b = a, c = to - 1, d = c;
            while ( true ) {
                int comparison;
                while ( b <= c && ( ( comparison = comp.compare( b, m ) ) <= 0 ) ) {
                    if ( comparison == 0 ) {
                        // Fix reference to pivot if necessary
                        if ( a == m ) m = b;
                        else if ( b == m ) m = a;
                        swapper.swap( a++, b );
                    }
                    b++;
                }
                while ( c >= b && ( ( comparison = comp.compare( c, m ) ) >= 0 ) ) {
                    if ( comparison == 0 ) {
                        // Fix reference to pivot if necessary
                        if ( c == m ) m = d;
                        else if ( d == m ) m = c;
                        swapper.swap( c, d-- );
                    }
                    c--;
                }
                if ( b > c ) break;
                // Fix reference to pivot if necessary
                if ( b == m ) m = d;
                else if ( c == m ) m = c;
                swapper.swap( b++, c-- );
            }

            // Swap partition elements back to middle
            s = Math.min( a - from, b - a );
            swap( swapper, from, b - s, s );
            s = Math.min( d - c, to - d - 1 );
            swap( swapper, b, to - s, s );

            // Recursively sort non-partition-elements
            int t;
            s = b - a;
            t = d - c;
            if ( s > 1 && t > 1 ) {
                invokeAll(
                    new FastUtilForkJoinQuickSort( from, from + s, comp, swapper ),
                    new FastUtilForkJoinQuickSort( to - t, to, comp, swapper )
                );
            } else if ( s > 1 ) {
                invokeAll( new FastUtilForkJoinQuickSort( from, from + s, comp, swapper ) );
            } else {
                invokeAll( new FastUtilForkJoinQuickSort( to - t, to, comp, swapper ) );
            }
        }
    }

}
