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

import java.time.LocalDate;
import java.util.stream.IntStream;

import org.testng.annotations.Test;
import static org.testng.Assert.*;

/**
 * Tuple tests
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class TupleTests {


    @Test
    public void testBasics() {
        for (int i=1; i<20; ++i) {
            final Object[] items = IntStream.range(0, i).boxed().toArray();
            final Tuple tuple = Tuple.of(items);
            System.out.println(tuple);
            assertEquals(tuple.size(), i);
            for (int j=0; j<tuple.size(); ++j) {
                int value = tuple.item(j);
                assertEquals(value, j);
            }
        }
    }


    @Test
    public void testHashCodeEquals1() {
        Tuple value1 = Tuple.of("One", "Two", "Three");
        Tuple value2 = Tuple.of("One", "Two", "Three");
        assertEquals(value1.hashCode(), value2.hashCode(), "Hashcode matches");
        assertTrue(value1 == value1.filter(0, value1.size()));
        assertTrue(value1.equals(value2));
        assertEquals(value1.item(0), "One");
        assertEquals(value1.item(1), "Two");
        assertEquals(value1.item(2), "Three");
    }

    @Test
    public void testHashCodeEquals2() {
        Tuple value1 = Tuple.of(1, "Two", "Three", 5d);
        Tuple value2 = Tuple.of(1, "Two", "Three", 5d);
        assertEquals(value1.hashCode(), value2.hashCode(), "Hashcode matches");
        assertTrue(value1 == value1.filter(0, value1.size()));
        assertTrue(value1.equals(value2));
        assertEquals(value1.item(0), new Integer(1));
        assertEquals(value1.item(1), "Two");
        assertEquals(value1.item(2), "Three");
        assertEquals(value1.item(3), 5d);
    }

    @Test()
    public void testSelecWithSize2() {
        Tuple value1 = Tuple.of(1, "Two");
        Tuple value2 = value1.filter(0, 2);
        assertTrue(value1 == value2);
        assertEquals(value2.size(), 2);
        assertEquals(value2.item(0), new Integer(1));
        assertEquals(value2.item(1), "Two");
    }

    @Test()
    public void testSelecWithSize6() {
        Tuple value1 = Tuple.of(1, "Two", "Three", 5d, LocalDate.now(), 5L);
        Tuple value2 = value1.filter(1, 2);
        assertTrue(value1 == value1.filter(0, value1.size()));
        assertEquals(value2.size(), 2);
        assertEquals(value2.item(0), "Two");
        assertEquals(value2.item(1), "Three");
    }

    @Test(expectedExceptions = {IllegalArgumentException.class })
    public void testInvalidOffset()  {
        Tuple.of(1, "Two", "Three", 5d, LocalDate.now(), 5L).filter(-1, 1);
    }

    @Test(expectedExceptions = {IllegalArgumentException.class })
    public void testInvalidLength()  {
        Tuple.of(1, "Two", "Three", 5d, LocalDate.now(), 5L).filter(0, 7);
    }



}
