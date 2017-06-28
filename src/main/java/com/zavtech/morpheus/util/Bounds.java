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

import java.util.Comparator;
import java.util.Optional;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * A simple class that captures the upper and lower bounds of some metric.
 *
 * @param <T>   the type for bounds
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class Bounds<T> implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private T lower;
    private T upper;

    /**
     * Constructor
     * @param lower     the lower bound
     * @param upper     the upper bound
     */
    private Bounds(T lower, T upper) {
        this.lower = lower;
        this.upper = upper;
    }


    /**
     * Returns a newly created bounds object for the values provided
     * @param lower     the lower bound
     * @param upper     the upper bound
     * @param <T>       the element type
     * @return          the newly created bounds
     */
    public static <T> Bounds<T> of(T lower, T upper) {
        return new Bounds<>(lower, upper);
    }

    /**
     * Returns the overall bounds given a set of sub-bounds
     * @param bounds    the bounds to compute overall bounds from
     * @param <T>       the element type
     * @return          the overall bounds
     */
    @SafeVarargs
    @SuppressWarnings("unchecked")
    public static <T> Bounds<T> ofAll(Bounds<T>... bounds) {
        if (bounds.length == 0) {
            throw new IllegalArgumentException("At least one Bounds entry must be provided");
        } else {
            T minValue = null;
            T maxValue = null;
            final Class<T> type = (Class<T>)bounds[0].lower.getClass();
            final Comparator<T> comparator = Comparators.getDefaultComparator(type);
            for (Bounds<T> value : bounds) {
                minValue = minValue == null ? value.lower : comparator.compare(minValue, value.lower) < 0 ? minValue : value.lower;
                maxValue = maxValue == null ? value.upper : comparator.compare(maxValue, value.upper) > 0 ? maxValue : value.upper;
            }
            return Bounds.of(minValue, maxValue);
        }
    }


    /**
     * Retruns the integer bounds of a stream of ints
     * @param stream    the stream to compute bounds on
     * @return          the bounds for stream, empty if no data in stream
     */
    public static Optional<Bounds<Integer>> ofInts(IntStream stream) {
        final OfInts calculator = new OfInts();
        stream.forEach(calculator::add);
        return calculator.getBounds();
    }


    /**
     * Retruns the integer bounds of a stream of longs
     * @param stream    the stream to compute bounds on
     * @return          the bounds for stream, empty if no data in stream
     */
    public static Optional<Bounds<Long>> ofLongs(LongStream stream) {
        final OfLongs calculator = new OfLongs();
        stream.forEach(calculator::add);
        return calculator.getBounds();
    }


    /**
     * Retruns the integer bounds of a stream of doubles
     * @param stream    the stream to compute bounds on
     * @return          the bounds for stream, empty if no data in stream
     */
    public static Optional<Bounds<Double>> ofDoubles(DoubleStream stream) {
        final OfDoubles calculator = new OfDoubles();
        stream.forEach(calculator::add);
        return calculator.getBounds();
    }


    /**
     * Retruns the integer bounds of a stream of doubles
     * @param stream    the stream to compute bounds on
     * @return          the bounds for stream, empty if no data in stream
     */
    @SuppressWarnings("unchecked")
    public static <V extends Comparable> Optional<Bounds<V>> ofValues(Stream<V> stream) {
        final OfComparables calculator = new OfComparables();
        stream.forEach(calculator::add);
        return calculator.getBounds();
    }


    /**
     * Returns the lower bound for this bounds definition
     * @return      the lower bound
     */
    public final T lower() {
        return lower;
    }

    /**
     * Returns the upper bound for this bounds definition
     * @return  the upper bound
     */
    public final T upper() {
        return upper;
    }

    @Override
    public int hashCode() {
        int result = lower != null ? lower.hashCode() : 0;
        result = 31 * result + (upper != null ? upper.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (other == null || getClass() != other.getClass()) return false;
        Bounds<?> bounds = (Bounds<?>) other;
        if (lower != null ? !lower.equals(bounds.lower) : bounds.lower != null) return false;
        return upper != null ? upper.equals(bounds.upper) : bounds.upper == null;

    }

    @Override
    public String toString() {
        return "Bounds{" +
                "lower=" + lower +
                ", upper=" + upper +
                '}';
    }


    /**
     * An interface to a bounds calculator
     * @param <T>   the type
     */
    interface BoundsCalculator<T> {

        /**
         * Returns the number of values consumed by this tracker
         * @return  the number of observations for this tracker
         */
        int count();

        /**
         * Returns the current bounds for this tracker
         * @return  the bounds, empty if count() == 0
         */
        Optional<Bounds<T>> getBounds();
    }

    /**
     * Implementation of BoundsCalculator for ints
     */
    private static class OfInts implements BoundsCalculator<Integer> {

        private int count;
        private int minValue = Integer.MAX_VALUE;
        private int maxValue = Integer.MIN_VALUE;

        /**
         * Adds an int to this bounds tracker
         * @param value the value to add
         * @return      the count for this tracker
         */
        public final int add(int value) {
            this.count++;
            this.minValue = Integer.compare(value, minValue) < 0 ? value : minValue;
            this.maxValue = Integer.compare(value, maxValue) > 0 ? value : maxValue;
            return count;
        }

        @Override
        public int count() {
            return count;
        }

        @Override()
        public Optional<Bounds<Integer>> getBounds() {
            return count == 0 ? Optional.empty() : Optional.of(Bounds.of(minValue, maxValue));
        }
    }

    /**
     * Implementation of BoundsCalculator for longs
     */
    private static class OfLongs implements BoundsCalculator<Long> {

        private int count;
        private long minValue = Long.MAX_VALUE;
        private long maxValue = Long.MIN_VALUE;

        /**
         * Adds an int to this bounds tracker
         * @param value the value to add
         * @return      the count for this tracker
         */
        public final int add(long value) {
            this.count++;
            this.minValue = Long.compare(value, minValue) < 0 ? value : minValue;
            this.maxValue = Long.compare(value, maxValue) > 0 ? value : maxValue;
            return count;
        }

        @Override
        public int count() {
            return count;
        }

        @Override()
        public Optional<Bounds<Long>> getBounds() {
            return count == 0 ? Optional.empty() : Optional.of(Bounds.of(minValue, maxValue));
        }
    }


    /**
     * Implementation of BoundsCalculator for doubles
     */
    private static class OfDoubles implements BoundsCalculator<Double> {

        private int count;
        private double minValue = Double.MAX_VALUE;
        private double maxValue = Double.MIN_VALUE;

        /**
         * Adds an int to this bounds tracker
         * @param value the value to add
         * @return      the count for this tracker
         */
        public final int add(double value) {
            if (!Double.isNaN(value)) {
                this.count++;
                this.minValue = Double.compare(value, minValue) < 0 ? value : minValue;
                this.maxValue = Double.compare(value, maxValue) > 0 ? value : maxValue;
            }
            return count;
        }

        @Override
        public int count() {
            return count;
        }

        @Override()
        public Optional<Bounds<Double>> getBounds() {
            return count == 0 ? Optional.empty() : Optional.of(Bounds.of(minValue, maxValue));
        }
    }


    /**
     * Implementation of BoundsCalculator for Comparables
     */
    private static class OfComparables<V extends Comparable> implements BoundsCalculator<V> {

        private int count;
        private V minValue = null;
        private V maxValue = null;

        /**
         * Adds an int to this bounds tracker
         * @param value the value to add
         * @return      the count for this tracker
         */
        @SuppressWarnings("unchecked")
        public final int add(V value) {
            if (value != null) {
                this.count++;
                this.minValue = minValue == null ? value : minValue.compareTo(value) > 0 ? value : minValue;
                this.maxValue = maxValue == null ? value : maxValue.compareTo(value) < 0 ? value : maxValue;
            }
            return count;
        }

        @Override
        public int count() {
            return count;
        }

        @Override()
        public Optional<Bounds<V>> getBounds() {
            return count == 0 ? Optional.empty() : Optional.of(Bounds.of(minValue, maxValue));
        }
    }

}
