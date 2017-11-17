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
package com.zavtech.morpheus.array;

import java.util.Random;
import java.util.function.Supplier;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests for array stats
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class ArrayStatsTests {

    private Array<Long> longs = Array.of(26L, 6L, 96L, 19L, 49L, 65L, 36L, 20L, 28L);
    private Array<Integer> integers = Array.of(72, 13, 85, 80, 32, 61, 39, 3, 31);
    private Array<Double> doubles = Array.of(45.20283042, 1.878915065, 41.74819734, 36.62664529, 71.09358412, 43.96346674,  42.91676983, 3.981916947, 81.9546373 );


    @DataProvider(name="styles")
    public Object[][] styles() {
        return new Object[][] {
            {ArrayStyle.DENSE},
            {ArrayStyle.SPARSE},
            {ArrayStyle.MAPPED}
        };
    }


    @Test()
    public void testCount() {
        Assert.assertEquals(integers.stats().count().intValue(), 9);
        Assert.assertEquals(longs.stats().count().intValue(), 9);
        Assert.assertEquals(doubles.stats().count().intValue(), 9);
    }


    @Test()
    public void testMin() {
        Assert.assertEquals(integers.stats().min().intValue(), 3);
        Assert.assertEquals(longs.stats().min().intValue(), 6);
        Assert.assertEquals(doubles.stats().min().doubleValue(), 1.878915065);
    }


    @Test()
    public void testMax() {
        Assert.assertEquals(integers.stats().max().intValue(), 85);
        Assert.assertEquals(longs.stats().max().intValue(), 96);
        Assert.assertEquals(doubles.stats().max().doubleValue(), 81.9546373);
    }


    @Test()
    public void testMean() {
        Assert.assertEquals(integers.stats().mean().doubleValue(), 46.22222222, 0.0000001);
        Assert.assertEquals(longs.stats().mean().doubleValue(), 38.3333333333, 0.0000001);
        Assert.assertEquals(doubles.stats().mean().doubleValue(), 41.0407736724, 0.0000001);
    }


    @Test()
    public void testMedian() {
        Assert.assertEquals(integers.stats().median().intValue(), 39);
        Assert.assertEquals(longs.stats().median().doubleValue(), 28.0d);
        Assert.assertEquals(doubles.stats().median().doubleValue(), 42.91676983d, 0.0000001);
    }


    @Test()
    public void testMad() {
        Assert.assertEquals(integers.stats().mad().doubleValue(), 25.1358024691, 0.0000001);
        Assert.assertEquals(longs.stats().mad().doubleValue(), 21.1111111111, 0.0000001);
        Assert.assertEquals(doubles.stats().mad().doubleValue(), 17.918854159, 0.0000001);
    }


    @Test()
    public void testStdDev() {
        Assert.assertEquals(integers.stats().stdDev().doubleValue(), 29.5498637, 0.0000001);
        Assert.assertEquals(longs.stats().stdDev().doubleValue(), 27.771388154, 0.0000001);
        Assert.assertEquals(doubles.stats().stdDev().doubleValue(), 26.2999402909, 0.0000001);
    }


    @Test()
    public void testSem() {
        Assert.assertEquals(integers.stats().sem().doubleValue(), 9.84995456529, 0.0000001);
        Assert.assertEquals(longs.stats().sem().doubleValue(), 9.25712938467, 0.0000001);
        Assert.assertEquals(doubles.stats().sem().doubleValue(), 8.76664676365, 0.0000001);
    }


    @Test()
    public void testSum() {
        Assert.assertEquals(integers.stats().sum().intValue(), 416);
        Assert.assertEquals(longs.stats().sum().intValue(), 345);
        Assert.assertEquals(doubles.stats().sum().doubleValue(), 369.366963052, 0.0000001);
    }


    @Test()
    public void testSumSquares() {
        Assert.assertEquals(integers.stats().sumSquares().intValue(), 26214);
        Assert.assertEquals(longs.stats().sumSquares().intValue(), 19395);
        Assert.assertEquals(doubles.stats().sumSquares().doubleValue(), 20692.60081, 0.00001);
    }


    @Test()
    public void testVariance() {
        Assert.assertEquals(integers.stats().variance().doubleValue(), 873.1944444, 0.0000001);
        Assert.assertEquals(longs.stats().variance().doubleValue(), 771.25, 0.0000001);
        Assert.assertEquals(doubles.stats().variance().doubleValue(), 691.686859307, 0.0000001);
    }


    @Test()
    public void testKurtosis() {
        Assert.assertEquals(integers.stats().kurtosis().doubleValue(), -1.45970131773, 0.0000001);
        Assert.assertEquals(longs.stats().kurtosis().doubleValue(), 1.26343941042, 0.0000001);
        Assert.assertEquals(doubles.stats().kurtosis().doubleValue(), -0.172687207843, 0.0000001);
    }


    @Test()
    public void testSkew() {
        Assert.assertEquals(integers.stats().skew().doubleValue(), -0.043532290202, 0.0000001);
        Assert.assertEquals(longs.stats().skew().doubleValue(), 1.2137650976, 0.0000001);
        Assert.assertEquals(doubles.stats().skew().doubleValue(), -0.135235437377, 0.0000001);
    }


    @Test()
    public void testGeoMean() {
        Assert.assertEquals(integers.stats().geoMean().doubleValue(), 32.8919966009, 0.0000001);
        Assert.assertEquals(longs.stats().geoMean().doubleValue(), 29.7527909458, 0.0000001);
        Assert.assertEquals(doubles.stats().geoMean().doubleValue(), 26.1331292389, 0.0000001);
    }


    @Test()
    public void testAutocorr() {
        Assert.assertEquals(integers.stats().autocorr(1).doubleValue(), -0.094644565373, 0.0000001);
        Assert.assertEquals(longs.stats().autocorr(1).doubleValue(), -0.389815713293, 0.0000001);
        Assert.assertEquals(doubles.stats().autocorr(1).doubleValue(), -0.410170021799, 0.0000001);
    }


    @Test()
    public void testPercentile() {
        Assert.assertEquals(integers.stats().percentile(0.8).doubleValue(), 75.2, 0.0000001);
        Assert.assertEquals(longs.stats().percentile(0.8).doubleValue(), 55.4, 0.0000001);
        Assert.assertEquals(doubles.stats().percentile(0.8).doubleValue(), 55.5591319, 0.0000001);
    }


    @Test()
    public void testProduct() {
        Assert.assertEquals(integers.stats().product().doubleValue(), 45062172979200d, 0.0000001);
        Assert.assertEquals(longs.stats().product().doubleValue(), 18270456422400d, 0.0000001);
        Assert.assertEquals(doubles.stats().product().doubleValue(), 5.684898905405635E+12, 0.0000001);
    }


    @Test(dataProvider = "styles")
    public void testCumSumOfInts(ArrayStyle style) {
        final Random random = new Random();
        final Supplier<Array<Integer>> factory = () -> {
            switch (style) {
                case DENSE:     return Array.of(Integer.class, 10000).applyInts(v -> random.nextInt(100));
                case SPARSE:    return Array.of(Integer.class, 10000, 0.8f).applyInts(v -> random.nextInt(100));
                case MAPPED:    return Array.map(Integer.class, 10000).applyInts(v -> random.nextInt(100));
                default:    throw new IllegalArgumentException("Unsupported style: " + style);
            }
        };
        final Array<Integer> source = factory.get();
        final Array<Integer> cumSum = source.cumSum();
        for (int i=0; i<source.length(); ++i) {
            final int actual = cumSum.getInt(i);
            final int expected = source.stats(0, i+1).sum().intValue();
            Assert.assertEquals(actual, expected, "Values match at index " + i);
        }
    }


    @Test(dataProvider = "styles")
    public void testCumSumOfLongs(ArrayStyle style) {
        final Random random = new Random();
        final Supplier<Array<Long>> factory = () -> {
            switch (style) {
                case DENSE:     return Array.of(Long.class, 10000).applyLongs(v -> (long)random.nextInt(100));
                case SPARSE:    return Array.of(Long.class, 10000, 0.8f).applyLongs(v -> (long)random.nextInt(100));
                case MAPPED:    return Array.map(Long.class, 10000).applyLongs(v -> (long)random.nextInt(100));
                default:    throw new IllegalArgumentException("Unsupported style: " + style);
            }
        };
        final Array<Long> source = factory.get();
        final Array<Long> cumSum = source.cumSum();
        for (int i=0; i<source.length(); ++i) {
            final long actual = cumSum.getLong(i);
            final long expected = source.stats(0, i+1).sum().longValue();
            Assert.assertEquals(actual, expected, "Values match at index " + i);
        }
    }


    @Test(dataProvider = "styles")
    public void testCumSumOfDoubles(ArrayStyle style) {
        final Random random = new Random();
        final Supplier<Array<Double>> factory = () -> {
            switch (style) {
                case DENSE:     return Array.of(Double.class, 10000).applyDoubles(v -> random.nextDouble() * 10d);
                case SPARSE:    return Array.of(Double.class, 10000, 0.8f).applyDoubles(v -> random.nextDouble() * 10d);
                case MAPPED:    return Array.map(Double.class, 10000).applyDoubles(v -> random.nextDouble() * 10d);
                default:    throw new IllegalArgumentException("Unsupported style: " + style);
            }
        };
        final Array<Double> source = factory.get();
        final Array<Double> cumSum = source.cumSum();
        for (int i=0; i<source.length(); ++i) {
            final double actual = cumSum.getDouble(i);
            final double expected = source.stats(0, i+1).sum().doubleValue();
            Assert.assertEquals(actual, expected, "Values match at index " + i);
        }
    }


    @Test(dataProvider = "styles")
    public void testCumSumOfDoublesWithNans(ArrayStyle style) {
        final Random random = new Random();
        final Supplier<Array<Double>> factory = () -> {
            switch (style) {
                case DENSE:     return Array.of(Double.class, 10000).applyDoubles(v -> random.nextDouble() * 10d);
                case SPARSE:    return Array.of(Double.class, 10000, 0.8f).applyDoubles(v -> random.nextDouble() * 10d);
                case MAPPED:    return Array.map(Double.class, 10000).applyDoubles(v -> random.nextDouble() * 10d);
                default:    throw new IllegalArgumentException("Unsupported style: " + style);
            }
        };
        final Array<Double> source = factory.get();
        source.setDouble(0, Double.NaN);
        source.setDouble(25, Double.NaN);
        final Array<Double> cumSum = source.cumSum();
        for (int i=0; i<source.length(); ++i) {
            final double actual = cumSum.getDouble(i);
            final double expected = source.stats(0, i+1).sum().doubleValue();
            if (i == 0) {
                Assert.assertTrue(Double.isNaN(actual));
            } else if (i == 25) {
                final double prior = source.stats(0, i).sum().doubleValue();
                Assert.assertEquals(actual, prior, "Values match at index " + i);
            } else {
                Assert.assertEquals(actual, expected, "Values match at index " + i);
            }
        }
    }




}
