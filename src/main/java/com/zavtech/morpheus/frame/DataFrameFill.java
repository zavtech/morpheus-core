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
 * An interface to copy values to fill nulls or NaNs in various directions.
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameFill {

    /**
     * Fills column data up if there are NaNs or Null values by copying up next value
     * @param intervals the maximum number of intervals to fill up
     * @return          the number of elements affected in the fill-up
     */
    int up(int intervals);

    /**
     * Fills column data down if there are NaNs or Null values by copying down previous value
     * @param intervals the maximum number of intervals to fill down
     * @return          the number of elements affected in the fill-down
     */
    int down(int intervals);

    /**
     * Fills row data right-to-left if there are NaNs or null values by copying values from the right adjacent cell
     * @param intervals the maximum number of intervals to fill left
     * @return          the number of elements affected by fill-left
     */
    int left(int intervals);

    /**
     * Fills row data left-to-right if there are NaNs or null values by copying values from the left adjacent cell
     * @param intervals the maximum number of intervals to fill left
     * @return          the number of elements affected by fill-left
     */
    int right(int intervals);

}
