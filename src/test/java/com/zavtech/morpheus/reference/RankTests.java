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

import java.util.Random;

import com.zavtech.morpheus.frame.DataFrame;
import org.apache.commons.math3.stat.ranking.NaturalRanking;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for the statistical rank functions on the DataFrame
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class RankTests {


    @Test()
    public void testRankOfRows() {
        final Random random = new Random();
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 1000, 100);
        frame.applyDoubles(v -> random.nextDouble() * 100);
        final DataFrame<String,String> rankFrame = frame.rank().ofRows();
        Assert.assertEquals(rankFrame.rowCount(), frame.rowCount(), "The row counts match");
        Assert.assertEquals(rankFrame.colCount(), frame.colCount(), "The column counts match");
        rankFrame.out().print();
        rankFrame.rows().forEach(rankRow -> {
            final String key = rankRow.key();
            final double[] values = frame.row(key).toDoubleStream().toArray();
            final NaturalRanking ranking = new NaturalRanking();
            final double[] ranks = ranking.rank(values);
            for (int i = 0; i < ranks.length; ++i) {
                final double value = frame.data().getDouble(key, i);
                final double expected = ranks[i];
                final double actual = rankFrame.data().getDouble(key, i);
                Assert.assertEquals(value, values[i], "The values match for column " + i);
                Assert.assertEquals(actual, expected, "The ranks match for " + key + " at column " + i);
            }
        });
    }


    @Test()
    public void testRankOfColumns() {
        final Random random = new Random();
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 1000, 100);
        frame.applyDoubles(v -> random.nextDouble() * 100);
        final DataFrame<String,String> rankFrame = frame.rank().ofColumns();
        Assert.assertEquals(rankFrame.rowCount(), frame.rowCount(), "The row counts match");
        Assert.assertEquals(rankFrame.colCount(), frame.colCount(), "The column counts match");
        rankFrame.out().print();
        rankFrame.cols().forEach(rankColumn -> {
            final String key = rankColumn.key();
            final double[] values = frame.col(key).toDoubleStream().toArray();
            final NaturalRanking ranking = new NaturalRanking();
            final double[] ranks = ranking.rank(values);
            for (int i=0; i<ranks.length; ++i) {
                final double value = frame.data().getDouble(i, key);
                final double expected = ranks[i];
                final double actual = rankFrame.data().getDouble(i, key);
                Assert.assertEquals(value, values[i], "The values match for row " + i);
                Assert.assertEquals(actual, expected, "The ranks match for " + key + " at row " + i);
            }
        });
    }
}
