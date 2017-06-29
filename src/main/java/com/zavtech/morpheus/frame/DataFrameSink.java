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

import java.util.function.Consumer;

/**
 * An interface to a sink that can write a DataFrame to some output device
 *
 * @param <O>   the options type for this sink
 * @param <R>   the DataFrame row key type
 * @param <C>   the DataFrame column key type
 *
 * <p>This is open source software released under the <a href="http://www.ap`ache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameSink<R,C,O> {

    /**
     * Writes a DataFrame to some output device using the options configurator provided
     * @param frame         the frame to write to output device
     * @param configurator  the options configurator to tailor output
     */
    void write(DataFrame<R,C> frame, Consumer<O> configurator);

}
