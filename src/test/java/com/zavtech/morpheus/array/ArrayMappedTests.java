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


import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests specific to memory mapped arrays
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class ArrayMappedTests {



    @Test()
    public void testBuild() {
        final int length = 10000;
        final Array<Double> dense = Array.of(Double.class, length).applyDoubles(v -> Math.random() * 100);
        final Array<Double> mapped = Array.map(Double.class, length, Double.NaN).applyDoubles(v -> dense.getDouble(v.index()));
        for (int i=0; i<mapped.length(); ++i) {
            final double v1 = dense.getDouble(i);
            final double v2 = mapped.getDouble(i);
            Assert.assertEquals(v1, v2, "Values match at index " + i);
        }
        for (int i=1; i<mapped.length(); ++i) {
            final double previous = mapped.getDouble(i-1);
            final double current = mapped.getDouble(i);
            Assert.assertTrue(previous != current);
        }
    }


    @Test()
    public void testCopy() {
        final int length = 10000;
        final Array<Double> mapped = Array.map(Double.class, length, Double.NaN).applyDoubles(v -> Math.random() * 100);
        final Array<Double> copy = mapped.copy();
        Assert.assertEquals(mapped.length(), copy.length());
        Assert.assertTrue(mapped != copy);
        for (int i= 0; i<mapped.length(); ++i) {
            final double v1 = mapped.getDouble(i);
            final double v2 = copy.getDouble(i);
            Assert.assertEquals(v1, v2, "Values match at index " + i);
        }
        mapped.sort(true);
        for (int i= 0; i<20; ++i) {
            final double v1 = mapped.getDouble(i);
            final double v2 = copy.getDouble(i);
            Assert.assertTrue(v1 != v2, "Values match at index " + i);
        }
        copy.sort(true);
        for (int i= 0; i<mapped.length(); ++i) {
            final double v1 = mapped.getDouble(i);
            final double v2 = copy.getDouble(i);
            Assert.assertEquals(v1, v2, "Values match at index " + i);
        }
    }


    @Test()
    public void testZonedDateTime() {
        final int count = 10;
        final Random random = new Random();
        final Array<ZonedDateTime> array1 = Array.map(ZonedDateTime.class, count, null);
        final ZonedDateTime[] array2 = new ZonedDateTime[count];
        for (int i=0; i<count; ++i) {
            final ZoneId zoneId = i % 2 == 0 ? ZoneId.of("UTC") : ZoneId.systemDefault();
            final ZonedDateTime value = ZonedDateTime.now().minusMinutes(random.nextInt(100)).withZoneSameInstant(zoneId);
            array1.setValue(i, value);
            array2[i] = value;
        }
        for (int i=0; i<count; ++i) {
            final ZonedDateTime v1 = array1.getValue(i);
            final ZonedDateTime v2 = array2[i];
            Assert.assertEquals(v1, v2);
            System.out.println(v1);
        }
    }
}
