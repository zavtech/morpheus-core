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
package com.zavtech.morpheus.frame;

import java.util.Iterator;
import java.util.function.Predicate;

/**
 * An interface to a provider of various types of iterators over <code>DataFrameValues</code>s
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameIterators<R,C> extends Iterable<DataFrameValue<R,C>> {

    /**
     * Returns an iterator over the values in this row
     * @return      the iterator over values in this row
     */
    Iterator<DataFrameValue<R,C>> iterator();

    /**
     * Returns an iterator over the values in this row that match the predicate
     * @param predicate     the predicate to match values
     * @return              the filtered value iterator
     */
    Iterator<DataFrameValue<R,C>> iterator(Predicate<DataFrameValue<R,C>> predicate);

}
