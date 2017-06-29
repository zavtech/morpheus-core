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
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * A test for DataFrame sorting functionality
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class ApplyTests {


    @Test()
    public void applyBooleans() {
        final DataFrame<String,String> frame = TestDataFrames.random(boolean.class, 100, 100);
        final DataFrame<String,String> sequential = frame.sequential();
        final DataFrame<String,String> parallel = frame.parallel();
        Assert.assertTrue(frame == sequential, "The original frame is sequential");
        Assert.assertTrue(sequential != parallel, "The parallel frame is a different reference");
        Assert.assertTrue(parallel.isParallel(), "The parallel frame is indeed in parallel mode");
        final Random random = new Random(235);
        final AtomicInteger sequentialCount = new AtomicInteger();
        sequential.applyBooleans(v -> {
            sequentialCount.incrementAndGet();
            return random.nextBoolean();
        });
        final AtomicInteger parallelCount = new AtomicInteger();
        parallel.applyBooleans(v -> {
            parallelCount.incrementAndGet();
            return sequential.data().getBoolean(v.rowKey(), v.colKey());
        });
        Assert.assertEquals(parallelCount.get(), sequentialCount.get(), "Both received same number of calls");
        DataFrameAsserts.assertEqualsByIndex(sequential, parallel);
    }


    @Test()
    public void applyInts() {
        final DataFrame<String,String> frame = TestDataFrames.random(int.class, 100, 100);
        final DataFrame<String,String> sequential = frame.sequential();
        final DataFrame<String,String> parallel = frame.parallel();
        Assert.assertTrue(frame == sequential, "The original frame is sequential");
        Assert.assertTrue(sequential != parallel, "The parallel frame is a different reference");
        Assert.assertTrue(parallel.isParallel(), "The parallel frame is indeed in parallel mode");
        final Random random = new Random(235);
        final AtomicInteger sequentialCount = new AtomicInteger();
        sequential.applyInts(v -> {
            sequentialCount.incrementAndGet();
            return random.nextInt();
        });
        final AtomicInteger parallelCount = new AtomicInteger();
        parallel.applyInts(v -> {
            parallelCount.incrementAndGet();
            return sequential.data().getInt(v.rowKey(), v.colKey());
        });
        Assert.assertEquals(parallelCount.get(), sequentialCount.get(), "Both received same number of calls");
        DataFrameAsserts.assertEqualsByIndex(sequential, parallel);
    }


    @Test()
    public void applyLongs() {
        final DataFrame<String,String> frame = TestDataFrames.random(long.class, 100, 100);
        final DataFrame<String,String> sequential = frame.sequential();
        final DataFrame<String,String> parallel = frame.parallel();
        Assert.assertTrue(frame == sequential, "The original frame is sequential");
        Assert.assertTrue(sequential != parallel, "The parallel frame is a different reference");
        Assert.assertTrue(parallel.isParallel(), "The parallel frame is indeed in parallel mode");
        final Random random = new Random(235);
        final AtomicInteger sequentialCount = new AtomicInteger();
        sequential.applyLongs(v -> {
            sequentialCount.incrementAndGet();
            return random.nextLong();
        });
        final AtomicInteger parallelCount = new AtomicInteger();
        parallel.applyLongs(v -> {
            parallelCount.incrementAndGet();
            return sequential.data().getLong(v.rowKey(), v.colKey());
        });
        Assert.assertEquals(parallelCount.get(), sequentialCount.get(), "Both received same number of calls");
        DataFrameAsserts.assertEqualsByIndex(sequential, parallel);
    }


    @Test()
    public void applyDoubles() {
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 100, 100);
        final DataFrame<String,String> sequential = frame.sequential();
        final DataFrame<String,String> parallel = frame.parallel();
        Assert.assertTrue(frame == sequential, "The original frame is sequential");
        Assert.assertTrue(sequential != parallel, "The parallel frame is a different reference");
        Assert.assertTrue(parallel.isParallel(), "The parallel frame is indeed in parallel mode");
        final Random random = new Random(235);
        final AtomicInteger sequentialCount = new AtomicInteger();
        sequential.applyDoubles(v -> {
            sequentialCount.incrementAndGet();
            return random.nextDouble();
        });
        final AtomicInteger parallelCount = new AtomicInteger();
        parallel.applyDoubles(v -> {
            parallelCount.incrementAndGet();
            return sequential.data().getDouble(v.rowKey(), v.colKey());
        });
        Assert.assertEquals(parallelCount.get(), sequentialCount.get(), "Both received same number of calls");
        DataFrameAsserts.assertEqualsByIndex(sequential, parallel);
    }


    @Test()
    public void applyValues() {
        final DataFrame<String,String> frame = TestDataFrames.random(Object.class, 100, 100);
        final DataFrame<String,String> sequential = frame.sequential();
        final DataFrame<String,String> parallel = frame.parallel();
        Assert.assertTrue(frame == sequential, "The original frame is sequential");
        Assert.assertTrue(sequential != parallel, "The parallel frame is a different reference");
        Assert.assertTrue(parallel.isParallel(), "The parallel frame is indeed in parallel mode");
        final Random random = new Random(235);
        final AtomicInteger sequentialCount = new AtomicInteger();
        sequential.applyValues(v -> {
            sequentialCount.incrementAndGet();
            return random.nextDouble();
        });
        final AtomicInteger parallelCount = new AtomicInteger();
        parallel.applyValues(v -> {
            parallelCount.incrementAndGet();
            return sequential.data().getValue(v.rowKey(), v.colKey());
        });
        Assert.assertEquals(parallelCount.get(), sequentialCount.get(), "Both received same number of calls");
        DataFrameAsserts.assertEqualsByIndex(sequential, parallel);
    }


}
