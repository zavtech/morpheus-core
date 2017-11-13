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

import java.util.List;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.stats.StatType;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Unit test for various statistics available through the Morpheus API.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class StatsTest1 {

    private static Index<String> rows = Index.of("R1", "R2", "R3", "R4");
    private static Index<String> columns = Index.of("C1", "C2", "C3", "C4", "C5", "C6");
    private static double[][] values = new double[][] {
        {  1.22d,  4.43d,     7.42d,  4.34d,    78.3d,  3003.49d},
        {  2.22d,  5.46d,  -103.67d,  3.45d,  -273.4d,  232.456d},
        {-23.45d,  6.45d,     9.48d,  2.35d,  0.4757d,   4.5667d},
        {  2.35d,  1.74d,     3.83d,  4.94d,   32.53d,    3.423d},
    };


    /**
     * Constructor
     */
    public StatsTest1() {
        super();
    }


    @DataProvider(name = "frames")
    public Object[][] createFrames() {
        final DataFrame<String,String> frame1 = DataFrame.of(rows, columns, Double.class);
        final DataFrame<String,String> frame3 = DataFrame.of(rows, columns, Object.class);
        frame1.applyDoubles(v -> values[v.rowOrdinal()][v.colOrdinal()]);
        frame3.applyDoubles(v -> values[v.rowOrdinal()][v.colOrdinal()]);
        return new Object[][] {
            { frame1 },
            { frame3 },
        };
    }


    @Test(dataProvider="frames")
    public void testSign(DataFrame<String, String> frame) {
        final DataFrame<String,String> result = frame.sign();
        frame.rows().keys().forEach(row -> frame.cols().keys().forEach(column -> {
            final double value = frame.data().getDouble(row, column);
            final int sign = result.data().getInt(row, column);
            if (value == 0d) assertEquals("Sign = 0 for " + row + ", " + column, 0, sign);
            if (value > 0d) assertEquals("Sign = 1 for " + row + ", " + column, 1, sign);
            if (value < 0d) assertEquals("Sign = -1 for " + row + ", " + column, -1, sign);
        }));
        frame.rows().forEach(row -> row.forEachValue(v -> {
            final String rowKey = v.rowKey();
            final String columnKey = v.colKey();
            final double value = frame.data().getDouble(rowKey, columnKey);
            final int sign = result.data().getInt(rowKey, columnKey);
            if (value == 0d) assertEquals("Sign = 0 for " + row + ", " + columnKey, 0, sign);
            if (value > 0d) assertEquals("Sign = 1 for " + row + ", " + columnKey, 1, sign);
            if (value < 0d) assertEquals("Sign = -1 for " + row + ", " + columnKey, -1, sign);
        }));
        frame.cols().forEach(column -> column.forEachValue(v -> {
            final String rowKey = v.rowKey();
            final String columnKey = v.colKey();
            final double value = frame.data().getDouble(rowKey, columnKey);
            final int sign = result.data().getInt(rowKey, columnKey);
            if (value == 0d) assertEquals("Sign = 0 for " + rowKey + ", " + columnKey, 0, sign);
            if (value > 0d) assertEquals("Sign = 1 for " + rowKey + ", " + columnKey, 1, sign);
            if (value < 0d) assertEquals("Sign = -1 for " + rowKey + ", " + columnKey, -1, sign);
        }));
    }


    @DataProvider(name="allStats")
    public Object[][] allStats() {
        final List<StatType> stats = StatType.univariate();
        final Object[][] args = new Object[stats.size()][1];
        for (int i=0; i<stats.size(); ++i) args[i][0] = stats.get(i);
        return args;
    }


    @Test(dataProvider="allStats")
    public void testStatFailsOnNumeric(StatType stat) {
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 100, 100);
        frame.cols().add("DateColumn", Object.class).applyValues(v -> "X:" + Math.random());
        switch (stat) {
            case MIN:               frame.stats().min();            break;
            case MAX:               frame.stats().max();            break;
            case MEAN:              frame.stats().mean();           break;
            case MEDIAN:            frame.stats().median();         break;
            case SUM:               frame.stats().sum();            break;
            case SEM:               frame.stats().sem();            break;
            case MAD:               frame.stats().mad();            break;
            case COUNT:             frame.stats().count();          break;
            case STD_DEV:           frame.stats().stdDev();         break;
            case SUM_LOGS:          frame.stats().sumLogs();        break;
            case SUM_SQUARES:       frame.stats().sumSquares();     break;
            case VARIANCE:          frame.stats().variance();       break;
            case KURTOSIS:          frame.stats().kurtosis();       break;
            case SKEWNESS:          frame.stats().skew();           break;
            case GEO_MEAN:          frame.stats().geoMean();        break;
            case PRODUCT:           frame.stats().product();        break;
            case PERCENTILE:        frame.stats().percentile(0.5d); break;
            case AUTO_CORREL:  frame.stats().autocorr(1);      break;
            default: throw new IllegalArgumentException("Unsupported stat specified: " + stat);
        }
    }

}
