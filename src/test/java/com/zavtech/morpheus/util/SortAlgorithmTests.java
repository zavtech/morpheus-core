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

import java.util.concurrent.ThreadLocalRandom;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests for the various supported sorting algorithms
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public class SortAlgorithmTests {


    @DataProvider(name="args")
    public Object[][] getArgs() {
        return new Object[][] {
            { SortAlgorithm.Type.FAST_UTIL, false },
            { SortAlgorithm.Type.FAST_UTIL, true },
        };
    }


    @Test(dataProvider = "args")
    public void testSortAscending(SortAlgorithm.Type type, boolean parallel) {
        final int[] values = ThreadLocalRandom.current().ints(5000000).toArray();
        final IntComparator comp = (i1, i2) -> Integer.compare(values[i1], values[i2]);
        final Swapper swapper = (i1, i2) -> { int x = values[i1]; values[i1] = values[i2]; values[i2] = x; };
        SortAlgorithm.of(type, parallel).sort(0, values.length, comp, swapper);
        for (int i=1; i<values.length; ++i) {
            final int v1 = values[i-1];
            final int v2 = values[i];
            Assert.assertTrue(Integer.compare(v1, v2) <= 0, "Values in ascending order at " + i);
        }
    }
}
