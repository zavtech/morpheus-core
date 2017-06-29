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
 * An interface to a movable cursor on a DataFrame which enables random access to DataFrame values via various typed methods.
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameCursor<R,C> extends DataFrameValue<R,C> {

    /**
     * Returns a copy of this cursor
     * @return  a copy of this cursor
     */
    DataFrameCursor<R,C> copy();

    /**
     * Moves this cursor to the same location as the value
     * @param value     the value with coordinates to move this cursor to
     * @return          this cursor
     */
    DataFrameCursor<R,C> moveTo(DataFrameValue value);

    /**
     * Moves this cursor to the row location specified, leaving column location unchanged
     * @param rowKey    the row key
     * @return          this cursor
     */
    DataFrameCursor<R,C> moveToRow(R rowKey);

    /**
     * Moves this cursor to the row location specified, leaving column location unchanged
     * @param rowOrdinal  the row index
     * @return          this cursor
     */
    DataFrameCursor<R,C> moveToRow(int rowOrdinal);

    /**
     * Moves this cursor to the column location specified, leaving row location unchanged
     * @param colKey    the column key
     * @return          this cursor
     */
    DataFrameCursor<R,C> moveToColumn(C colKey);

    /**
     * Moves this cursor to the column location specified, leaving row location unchanged
     * @param colOrdinal    the column ordinal
     * @return              this cursor
     */
    DataFrameCursor<R,C> moveToColumn(int colOrdinal);

    /**
     * Moves this cursor to the location specified
     * @param rowKey    the row key
     * @param colKey    the column key
     * @return          this cursor
     */
    DataFrameCursor<R,C> moveTo(R rowKey, C colKey);

    /**
     * Moves this cursor to the location specified
     * @param rowOrdinal    the row ordinal
     * @param colKey        the row key
     * @return              this cursor
     */
    DataFrameCursor<R,C> moveTo(int rowOrdinal, C colKey);

    /**
     * Moves this cursor to the location specified
     * @param rowKey        the row key
     * @param colOrdinal    the column ordinal
     * @return              this cursor
     */
    DataFrameCursor<R,C> moveTo(R rowKey, int colOrdinal);

    /**
     * Moves this cursor to the location specified
     * @param rowOrdinal    the row ordinal
     * @param colOrdinal    the column ordinal
     * @return              this cursor
     */
    DataFrameCursor<R,C> moveTo(int rowOrdinal, int colOrdinal);

}
