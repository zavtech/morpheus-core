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
package com.zavtech.morpheus.stats;

/**
 * An interface used to abstract a sample of double precision data that can be passed to the various statistical functions.
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface Sample {

    /**
     * Returns the value in the sample at the index specified
     * @param index     the index for requested value
     * @return          the value at the index
     */
    double getDouble(int index);

    /**
     * Returns a Sample that wraps the double array
     * @param values    the value array to wrap
     * @return          the sample wrapper
     */
    static Sample of(double... values) {
        return index -> values[index];
    }
}
