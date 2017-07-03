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

import java.io.File;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.zavtech.morpheus.TestSuite;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameColumn;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.stats.Stats;
import com.zavtech.morpheus.util.PerfStat;

/**
 * Unit test on the data frame of quote data.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class QuoteTests {

    private static Map<String,Double> meanMap = new HashMap<>();
    private static Map<String,Double> stdDevSSMap = new HashMap<>();
    private static Map<String,Double> stdDevPPMap = new HashMap<>();
    private static Map<String,Double> medianMap = new HashMap<>();

    /**
     * Static initializer
     */
    static {
        meanMap.put("Open", 529.8729204);
        meanMap.put("High", 534.7123894);
        meanMap.put("Low", 524.2312389);
        meanMap.put("Close", 529.4233097);
        meanMap.put("Volume", 112289609.2);

        stdDevSSMap.put("Open", 70.75617642);
        stdDevSSMap.put("High", 70.77320621);
        stdDevSSMap.put("Low", 69.96391932);
        stdDevSSMap.put("Close", 70.49638631);
        stdDevSSMap.put("Volume", 53259577.17);

        stdDevPPMap.put("Open", 70.69353261);
        stdDevPPMap.put("High", 70.71054732);
        stdDevPPMap.put("Low", 69.90197693);
        stdDevPPMap.put("Close", 70.43397249);
        stdDevPPMap.put("Volume", 53212423.92);

        medianMap.put("Open", 529.06);
        medianMap.put("High", 532.75);
        medianMap.put("Low", 523.30);
        medianMap.put("Close", 529.82);
        medianMap.put("Volume", 97936300d);

    }

    @Test()
    public void testPrint() throws Exception {
        TestDataFrames.getQuotes("blk").tail(20).out().print(30);
    }


    @Test()
    public void testStats() throws Exception {
        final DataFrame<LocalDate,String> frame = DataFrame.read().csv(options -> {
            options.setExcludeColumns("Date");
            options.setResource("/quotes/quote.csv");
            options.setRowKeyParser(LocalDate.class, values -> LocalDate.parse(values[0]));
        });
        frame.cols().keys().forEach(columnKey -> {
            if (meanMap.containsKey(columnKey)) {
                final DataFrameColumn<LocalDate,String> column = frame.colAt(columnKey);
                final Stats<Double> stats1 = column.stats();
                Assert.assertEquals(stats1.mean(), meanMap.get(columnKey), 0.01, "Mean of column " + columnKey);
                Assert.assertEquals(stats1.stdDev(), stdDevSSMap.get(columnKey), 0.01, "StdDev of column " + columnKey);
                Assert.assertEquals(stats1.median(), medianMap.get(columnKey), 0.01, "Median of column " + columnKey);

                final Stats<Double> stats2 = frame.colAt(columnKey).stats();
                Assert.assertEquals(stats2.mean(), meanMap.get(columnKey), 0.01, "Mean of column " + columnKey);
                Assert.assertEquals(stats2.stdDev(), stdDevSSMap.get(columnKey), 0.01, "StdDev of column " + columnKey);
                Assert.assertEquals(stats2.median(), medianMap.get(columnKey), 0.01, "Median of column " + columnKey);
            }
        });
    }


    @Test()
    public void testRowStats() throws Exception {
        final DataFrame<LocalDate,String> frame = DataFrame.read().csv(options -> {
            options.setExcludeColumns("Date");
            options.setResource("/quotes/quote.csv");
            options.setRowKeyParser(LocalDate.class, values -> LocalDate.parse(values[0]));
        });
        for (int i=0; i<10; ++i) {
            final long t1 = System.currentTimeMillis();
            final double[] sum = new double[1];
            frame.rows().keys().forEach(rowKey -> {
                final Stats<Double> stats = frame.rowAt(rowKey).stats();
                final double mean = stats.mean();
                final double stddev = stats.stdDev();
                final double min = stats.min();
                final double max = stats.max();
                sum[0] = mean + stddev + min + max;

            });
            final long t2 = System.currentTimeMillis();
            System.out.println("Completed " + frame.rows().count() + " rows in " + (t2-t1) + " millis");
        }
    }


    @Test()
    public void testColumnStats() throws Exception {
        final DataFrame<LocalDate,String> frame = DataFrame.read().csv(options -> {
            options.setExcludeColumns("Date");
            options.setResource("/quotes/quote.csv");
            options.setRowKeyParser(LocalDate.class, values -> LocalDate.parse(values[0]));
        });
        PerfStat.timeInMicros("Column stats", 20, () -> {
            final double[] sum = new double[1];
            frame.cols().keys().forEach(columnKey -> {
                final Stats<Double> stats = frame.colAt(columnKey).stats();
                final double mean = stats.mean();
                final double stddev = stats.stdDev();
                final double min = stats.min();
                final double max = stats.max();
                sum[0] = mean + stddev + min + max;
            });
            return frame;
        });
    }

    public void testRowDemean() throws Exception {
        final DataFrame<LocalDate,String> frame = DataFrame.read().csv(options -> {
            options.setExcludeColumns("Date");
            options.setResource("/quotes/quote.csv");
            options.setRowKeyParser(LocalDate.class, values -> LocalDate.parse(values[0]));
        });
        PerfStat.timeInMillis("Row demean", 20, () -> {
            frame.rows().keys().forEach(rowKey -> {
                final double mean = frame.rowAt(rowKey).stats().mean();
                frame.rowAt(rowKey).applyValues(v -> v.getDouble() - mean);
            });
            return frame;
        });
    }


    @Test()
    public void testCrossSectionalReturns() throws Exception {
        LocalDate startDate = LocalDate.MIN;
        LocalDate endDate = LocalDate.MAX;
        final Index<LocalDate> rowKeys = Index.of(LocalDate.class, 100);
        final Index<String> tickers = Index.of("BLK", "CSCO", "SPY", "YHOO", "VNQI", "VGLT", "VCLT");
        final DataFrame<LocalDate,String> closePrices = DataFrame.ofDoubles(rowKeys, tickers);
        for (String ticker : tickers) {
            System.out.println("Loading data for ticker " + ticker);
            final DataFrame<LocalDate,String> quotes = TestDataFrames.getQuotes(ticker);
            quotes.tail(10).out().print();
            closePrices.rows().addAll(quotes.rows().keyArray());
            final LocalDate firstKey = quotes.rows().firstKey().get();
            final LocalDate lastKey = quotes.rows().lastKey().get();
            startDate = firstKey.isAfter(startDate) ? firstKey : startDate;
            endDate = lastKey.isBefore(endDate) ? lastKey : endDate;
            quotes.rows().forEach(row -> {
                final LocalDate date = row.key();
                final double price = row.getDouble("Adj Close");
                closePrices.data().setDouble(date, ticker, price);
            });
        }

        final Set<LocalDate> nanDates = new HashSet<>();
        closePrices.rows().forEach(row -> row.forEachValue(v -> {
            final double value = v.getDouble();
            if (Double.isNaN(value)) {
                final LocalDate rowKey = row.key();
                nanDates.add(rowKey);
                if (rowKey.getYear() == 2014) {
                    System.out.println(rowKey);
                }
            }
        }));

        final DataFrame<LocalDate,String> selection = closePrices.rows().select(row -> !nanDates.contains(row.key()));
        final DataFrame<LocalDate,String> sorted = selection.rows().sort((row1, row2) -> row1.key().compareTo(row2.key()));
        final DataFrame<LocalDate,String> returns = sorted.calc().percentChanges();
        returns.rows().first().get().applyDoubles(v -> 0d);
        returns.head(10).out().print();
        returns.cols().stats().correlation().out().print();
    }


    @Test()
    public void testSimpleMovingAverage() throws Exception {
        final File file = TestSuite.getOutputFile("quote-tests", "sma.csv");
        final DataFrame<LocalDate,String> quotes = TestDataFrames.getQuotes("blk");
        final DataFrame<LocalDate,String> prices = quotes.cols().select(column -> !column.key().equalsIgnoreCase("Volume"));
        final DataFrame<LocalDate,String> sma = prices.calc().sma(50).cols().mapKeys(col -> col.key() + "(SMA)");
        sma.update(prices, false, true);
        sma.write().csv(options -> {
            options.setFile(file);
        });
    }

}
