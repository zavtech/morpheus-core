/**
 * Copyright (C) 2010-2014 Sebastiano Vigna
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.zavtech.morpheus.util;

/**
 * An interface to an ordered collection that can swap elements whose position is specified by integers
 */
public interface Swapper {

    /**
     * Swaps the data at the given positions.
     * @param index1  the first position to swap.
     * @param index2  the second position to swap.
     */
    void swap(int index1, int index2);
}