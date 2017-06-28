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
 * An interface that provides a convenient API to cap values in a DataFrame via type specific primitive methods.
 *
 * @param <R>   the frame row key type
 * @param <C>   the frame column key type
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameCap<R,C> {

    /**
     * Returns a DataFrame after capping the values with the bounds provided
     * @param lower     the lower bound
     * @param upper     the upper bound
     * @return          the DataFrame of capped values
     */
    DataFrame<R,C> ints(int lower, int upper);

    /**
     * Returns a DataFrame after capping the values with the bounds provided
     * @param lower     the lower bound
     * @param upper     the upper bound
     * @return          the DataFrame of capped values
     */
    DataFrame<R,C> longs(int lower, int upper);

    /**
     * Returns a DataFrame after capping the values with the bounds provided
     * @param lower     the lower bound
     * @param upper     the upper bound
     * @return          the DataFrame of capped values
     */
    DataFrame<R,C> doubles(double lower, double upper);

    /**
     * Returns a DataFrame after capping the values with the bounds provided
     * @param lower     the lower bound
     * @param upper     the upper bound
     * @return          the DataFrame of capped values
     */
    <T extends Comparable> DataFrame<R,C> Values(T lower, T upper);

}
