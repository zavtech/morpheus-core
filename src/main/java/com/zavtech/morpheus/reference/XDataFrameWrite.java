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
package com.zavtech.morpheus.reference;

import java.util.Objects;
import java.util.function.Consumer;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameSink;
import com.zavtech.morpheus.frame.DataFrameWrite;
import com.zavtech.morpheus.sink.CsvSink;
import com.zavtech.morpheus.sink.CsvSinkOptions;
import com.zavtech.morpheus.sink.DbSink;
import com.zavtech.morpheus.sink.DbSinkOptions;
import com.zavtech.morpheus.sink.JsonSink;
import com.zavtech.morpheus.sink.JsonSinkOptions;

/**
 * The reference implementation of the DataFrameWrite interface to enable DataFrames to be written out to a storage device.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class XDataFrameWrite<R,C> implements DataFrameWrite<R,C> {

    private DataFrame<R,C> frame;

    /**
     * Constructor
     * @param frame the frame reference
     */
    XDataFrameWrite(DataFrame<R,C> frame) {
        this.frame = frame;
    }

    @Override
    public final void db(Consumer<DbSinkOptions<R,C>> configurator) {
        this.to(new DbSink<>(), configurator);
    }

    @Override
    public final void csv(Consumer<CsvSinkOptions<R>> configurator) {
        this.to(new CsvSink<>(), configurator);
    }

    @Override
    public final void json(Consumer<JsonSinkOptions> configurator) {
        this.to(new JsonSink<>(), configurator);
    }

    @Override
    public final <O,S extends DataFrameSink<R,C,O>> void to(S sink, Consumer<O> configurator) {
        Objects.requireNonNull(sink, "The sink cannot be null");
        Objects.requireNonNull(configurator, "The options configurator cannot be null");
        sink.write(frame, configurator);
    }

}
