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

import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * An interface that provides Java 8 streams of various types for elements in the underlying array
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public interface ArrayStreams<T> {

    /**
     * Returns the stream of values, boxing primitives if necessary
     * @return      the stream of values, boxing primitives if necessary
     */
    Stream<T> values();

    /**
     * Returns a stream of primitive integers
     * @return  the stream of primitive integers
     * @throws ArrayException   if array type does not support ints
     */
    IntStream ints() throws ArrayException;

    /**
     * Returns a stream of primitive longs
     * @return  the stream of primitive longs
     * @throws ArrayException   if array type does not support longs
     */
    LongStream longs() throws ArrayException;

    /**
     * Returns a stream of primitive doubles
     * @return  the stream of primitive doubles
     * @throws ArrayException   if array type does not support doubles
     */
    DoubleStream doubles() throws ArrayException;

}
