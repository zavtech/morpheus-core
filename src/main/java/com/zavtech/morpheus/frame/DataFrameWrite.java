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

import com.zavtech.morpheus.sink.CsvSinkOptions;
import com.zavtech.morpheus.sink.DbSinkOptions;
import com.zavtech.morpheus.sink.JsonSinkOptions;

/**
 * An interface that can be used to write a DataFrame to an output device for storage or network transfer.
 *
 * <p>This is open source software released under the <a href="http://www.ap`ache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameWrite<R,C> {

    /**
     * Writes the DataFrame associated with this function to a SQL database
     * @param configurator  the configurator to apply DB options
     */
    void db(Consumer<DbSinkOptions<R,C>> configurator);

    /**
     * Writes the DataFrame associated with this function to CSV output
     * @param configurator  the configurator to apply CSV options
     */
    void csv(Consumer<CsvSinkOptions<R>> configurator);

    /**
     * Writes the DataFrame associated with this function to JSON output
     * @param configurator  the configurator to apply JSON options
     */
    void json(Consumer<JsonSinkOptions> configurator);

    /**
     * Writes the DataFrame associated with this function to an output device
     * @param sink          the sink instance to write to
     * @param configurator  the sink specific options configurator
     * @param <O>           the options type
     * @param <S>           the sink type
     */
    <O,S extends DataFrameSink<R,C,O>> void to(S sink, Consumer<O> configurator);

}
