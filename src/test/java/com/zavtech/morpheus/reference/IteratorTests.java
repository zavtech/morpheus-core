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
import java.util.concurrent.atomic.AtomicInteger;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameValue;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for the DataFrameIterators interface
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class IteratorTests {


    @Test()
    public void testIteratorOfBooleans() {
        final AtomicInteger counter = new AtomicInteger();
        final DataFrame<String,String> frame = TestDataFrames.random(boolean.class, 100, 100);
        frame.iterator().forEachRemaining(v -> {
            final boolean v1 = v.getBoolean();
            final boolean v2 = frame.data().getBoolean(v.rowOrdinal(), v.colOrdinal());
            counter.incrementAndGet();
            Assert.assertEquals(v1, v2, "Values match at " + v.rowOrdinal() + "," + v.colOrdinal());
        });
        Assert.assertEquals(counter.get(), 100 * 100);
    }


    @Test()
    public void testIteratorOfInts() {
        final AtomicInteger counter = new AtomicInteger();
        final DataFrame<String,String> frame = TestDataFrames.random(int.class, 100, 100);
        frame.iterator().forEachRemaining(v -> {
            final int v1 = v.getInt();
            final int v2 = frame.data().getInt(v.rowOrdinal(), v.colOrdinal());
            counter.incrementAndGet();
            Assert.assertEquals(v1, v2, "Values match at " + v.rowOrdinal() + "," + v.colOrdinal());
        });
        Assert.assertEquals(counter.get(), 100 * 100);
    }


    @Test()
    public void testIteratorOfLongs() {
        final AtomicInteger counter = new AtomicInteger();
        final DataFrame<String,String> frame = TestDataFrames.random(long.class, 100, 100);
        frame.iterator().forEachRemaining(v -> {
            final long v1 = v.getLong();
            final long v2 = frame.data().getLong(v.rowOrdinal(), v.colOrdinal());
            counter.incrementAndGet();
            Assert.assertEquals(v1, v2, "Values match at " + v.rowOrdinal() + "," + v.colOrdinal());
        });
        Assert.assertEquals(counter.get(), 100 * 100);
    }


    @Test()
    public void testIteratorOfDoubles() {
        final AtomicInteger counter = new AtomicInteger();
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 100, 100);
        frame.iterator().forEachRemaining(v -> {
            final double v1 = v.getDouble();
            final double v2 = frame.data().getDouble(v.rowOrdinal(), v.colOrdinal());
            counter.incrementAndGet();
            Assert.assertEquals(v1, v2, "Values match at " + v.rowOrdinal() + "," + v.colOrdinal());
        });
        Assert.assertEquals(counter.get(), 100 * 100);
    }


    @Test()
    public void testIteratorOfValues() {
        final AtomicInteger counter = new AtomicInteger();
        final DataFrame<String,String> frame = TestDataFrames.random(String.class, 100, 100);
        frame.iterator().forEachRemaining(v -> {
            final String v1 = v.getValue();
            final String v2 = frame.data().getValue(v.rowOrdinal(), v.colOrdinal());
            counter.incrementAndGet();
            Assert.assertEquals(v1, v2, "Values match at " + v.rowOrdinal() + "," + v.colOrdinal());
        });
        Assert.assertEquals(counter.get(), 100 * 100);
    }


    @Test()
    public void testIteratorOfBooleansWithPredicate() {
        final AtomicInteger counter = new AtomicInteger();
        final DataFrame<String,String> frame = TestDataFrames.random(boolean.class, 100, 100);
        frame.iterator(DataFrameValue::getBoolean).forEachRemaining(v -> {
            final boolean v1 = v.getBoolean();
            counter.incrementAndGet();
            Assert.assertTrue(v1, "Values match at " + v.rowOrdinal() + "," + v.colOrdinal());
        });
        Assert.assertTrue(counter.get() > 0, "There was at least one match");
    }


    @Test()
    public void testIteratorOfIntsWithPredicate() {
        final Random random = new Random();
        final AtomicInteger counter = new AtomicInteger();
        final DataFrame<String,String> frame = TestDataFrames.random(int.class, 100, 100);
        frame.applyInts(v -> random.nextInt() * 5);
        frame.iterator(v -> v.getInt() > 3).forEachRemaining(v -> {
            final int v1 = v.getInt();
            counter.incrementAndGet();
            Assert.assertTrue(v1 > 3, "Values match at " + v.rowOrdinal() + "," + v.colOrdinal());
        });
        Assert.assertTrue(counter.get() > 0, "There was at least one match");
    }


    @Test()
    public void testIteratorOfLongsWithPredicate() {
        final Random random = new Random();
        final AtomicInteger counter = new AtomicInteger();
        final DataFrame<String,String> frame = TestDataFrames.random(long.class, 100, 100);
        frame.applyLongs(v -> random.nextLong() * 5);
        frame.iterator(v -> v.getLong() > 3).forEachRemaining(v -> {
            final long v1 = v.getLong();
            counter.incrementAndGet();
            Assert.assertTrue(v1 > 3, "Values match at " + v.rowOrdinal() + "," + v.colOrdinal());
        });
        Assert.assertTrue(counter.get() > 0, "There was at least one match");
    }


    @Test()
    public void testIteratorOfDoublesWithPredicate() {
        final Random random = new Random();
        final AtomicInteger counter = new AtomicInteger();
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 100, 100);
        frame.applyDoubles(v -> random.nextDouble() * 5);
        frame.iterator(v -> v.getDouble() > 3).forEachRemaining(v -> {
            final double v1 = v.getDouble();
            counter.incrementAndGet();
            Assert.assertTrue(v1 > 3d, "Values match at " + v.rowOrdinal() + "," + v.colOrdinal());
        });
        Assert.assertTrue(counter.get() > 0, "There was at least one match");
    }


    @Test()
    public void testIteratorOfValuesWithPredicate() {
        final AtomicInteger counter = new AtomicInteger();
        final DataFrame<String,String> frame = TestDataFrames.random(String.class, 100, 100);
        frame.applyValues(v -> "" + v.rowOrdinal());
        frame.iterator(v -> !v.getValue().equals("4")).forEachRemaining(v -> {
            final String v1 = v.getValue();
            counter.incrementAndGet();
            Assert.assertTrue(!v1.equals("4"), "Values match at " + v.rowOrdinal() + "," + v.colOrdinal());
        });
        Assert.assertTrue(counter.get() > 0, "There was at least one match");
    }


}
