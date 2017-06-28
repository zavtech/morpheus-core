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

/**
 * An extension of the <code>ArrayValue</code> interface to provide a movable cursor on the Array.
 *
 * @param <T>   the Array element type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public interface ArrayCursor<T> extends ArrayValue<T> {

    /**
     * Returns a copy of this cursor
     * @return  copy of this cursor
     */
    ArrayCursor<T> copy();

    /**
     * Movies this cursor to the array index specified
     * @param index     the array index
     * @return          this cursor
     */
    ArrayCursor<T> moveTo(int index);

}
