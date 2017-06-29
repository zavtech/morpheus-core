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

/**
 * An interface to calculate the rank of row or column data in a DataFrame
 *
 * @see <a href="http://en.wikipedia.org/wiki/Ranking">Wikipedia</a>
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameRank<R,C> {

    /**
     * Returns a <code>DataFrame</code> of the same dimension containing row ranked data
     * @return      the <code>DataFrame</code> of ranked data
     * @throws DataFrameException      if data is non numeric
     * @see <span>For details, see <a href="http://en.wikipedia.org/wiki/Ranking">Wikiepdia Reference</a></span>
     */
    DataFrame<R,C> ofRows() throws DataFrameException;

    /**
     * Returns a <code>DataFrame</code> of the same dimension containing column ranked data
     * @return      the <code>DataFrame</code> of ranked data
     * @throws DataFrameException      if data is non numeric
     * @see <span>For details, see <a href="http://en.wikipedia.org/wiki/Ranking">Wikiepdia Reference</a></span>
     */
    DataFrame<R,C> ofColumns() throws DataFrameException;

}
