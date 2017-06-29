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
 * An interface to expose data smoothing functions to remove noise from column data in a DataFrame
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p>This is open source software released under the <a href="http://www.ap`ache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameSmooth<R,C> {

    /**
     * Applies a Simple Moving Average filter to the column data in the DataFame
     * @param windowSize    the window size for moving average
     * @return              the updated DataFrame
     */
    DataFrame<R,C> sma(double windowSize);

    /**
     * Applies an Exponentially Weighted Moving Average filter to the column data in the DataFame
     * @param halfLife  the half life for EWMA smoothing
     * @return              the updated DataFrame
     */
    DataFrame<R,C> ema(double halfLife);

}
