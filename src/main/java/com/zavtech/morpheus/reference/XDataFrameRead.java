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

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameRead;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.source.CsvSource;
import com.zavtech.morpheus.source.CsvSourceOptions;
import com.zavtech.morpheus.source.DbSource;
import com.zavtech.morpheus.source.DbSourceOptions;
import com.zavtech.morpheus.source.JsonSource;
import com.zavtech.morpheus.source.JsonSourceOptions;
import com.zavtech.morpheus.util.Asserts;
import com.zavtech.morpheus.util.Initialiser;

/**
 * The default implementation of the DataFrame read interface
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class XDataFrameRead implements DataFrameRead {

    private Set<DataFrameSource<?,?,?>> sourceSet = new HashSet<>();

    /**
     * Constructor
     */
    XDataFrameRead() {
        this.register(new CsvSource<>());
        this.register(new JsonSource<>());
        this.register(new DbSource<>());
    }

    /**
     * Registers a source that can be used to load frame some underlying device
     * @param source    the source instance
     */
    public void register(DataFrameSource<?,?,?> source) {
        Asserts.notNull(source, "The DataFrameSource cannot be null");
        this.sourceSet.add(source);
    }

    @Override
    public <R> DataFrame<R, String> csv(File file) {
        return csv(options -> options.setFile(file));
    }

    @Override
    public <R> DataFrame<R, String> csv(URL url) {
        return csv(options -> options.setURL(url));
    }

    @Override
    public <R> DataFrame<R, String> csv(InputStream is) {
        return csv(options -> options.setInputStream(is));
    }

    @Override
    public <R> DataFrame<R,String> csv(String resource) {
        return csv(options -> options.setResource(resource));
    }

    @Override
    public <R> DataFrame<R, String> csv(Consumer<CsvSourceOptions<R>> configurator) {
        return apply(Initialiser.apply(new CsvSourceOptions<>(), configurator));
    }

    @Override
    public <R, C> DataFrame<R, C> json(File file) {
        return json(options -> options.setFile(file));
    }

    @Override
    public <R, C> DataFrame<R, C> json(URL url) {
        return json(options -> options.setURL(url));
    }

    @Override
    public <R, C> DataFrame<R, C> json(InputStream is) {
        return json(options -> options.setInputStream(is));
    }

    @Override
    public <R,C> DataFrame<R,C> json(String resource) {
        return json(options -> options.setResource(resource));
    }

    @Override
    public <R,C> DataFrame<R,C> json(Consumer<JsonSourceOptions<R,C>> configurator) {
        return apply(Initialiser.apply(new JsonSourceOptions<>(), configurator));
    }

    @Override
    public <R> DataFrame<R, String> db(Consumer<DbSourceOptions<R>> configurator) {
        return apply(Initialiser.apply(new DbSourceOptions<>(), configurator));
    }

    @Override
    public <R,C,O extends DataFrameSource.Options<R, C>> DataFrame<R,C> apply(Class<O> type, Consumer<O> configurator) {
        try {
            Asserts.notNull(type, "The options type cannot be null");
            Asserts.notNull(configurator, "The configurator cannot be null");
            return apply(Initialiser.apply(type.newInstance(), configurator));
        } catch (InstantiationException ex) {
            throw new DataFrameException("Failed to create DataFrame source options for type: " + type.getName(), ex);
        } catch (IllegalAccessException ex) {
            throw new DataFrameException("Failed to access DataFrame source options for type: " + type.getName(), ex);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R,C,O extends DataFrameSource.Options<R,C>> DataFrame<R,C> apply(O options) {
        Asserts.notNull(options, "The options type cannot be null");
        final List<DataFrameSource<?,?,?>> matches = sourceSet.stream().filter(s -> s.isSupported(options)).collect(Collectors.toList());
        if (matches.size() == 1) {
            return ((DataFrameSource<R,C,O>)matches.iterator().next()).read(options);
        } else if (matches.size() == 0) {
            throw new DataFrameException("No DataFrameSources registered with support for " + options.getClass().getSimpleName());
        } else {
            throw new DataFrameException("Multiple DataFrameSources registered with support for " + options.getClass().getSimpleName());
        }
    }
}
