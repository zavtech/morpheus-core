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
import java.util.function.Consumer;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameRead;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.source.*;

/**
 * The default implementation of the DataFrame read interface
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class XDataFrameRead implements DataFrameRead {

    /**
     * Static initializer
     */
    static {
        DataFrameSource.register(new CsvSource<>());
        DataFrameSource.register(new JsonSource<>());
        DataFrameSource.register(new DbSource<>());
        DataFrameSource.register(new ExcelSource<>());
    }

    /**
     * Constructor
     */
    XDataFrameRead() {
        super();
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
    @SuppressWarnings("unchecked")
    public <R> DataFrame<R,String> csv(Consumer<CsvSourceOptions<R>> configurator) {
        return DataFrameSource.lookup(CsvSource.class).read(configurator);
    }

    @Override
    public <R> DataFrame<R, String> excel(InputStream is) {
        return excel(options -> options.setInputStream(is));
    }

    @Override
    public <R> DataFrame<R,String> excel(URL url) { return excel(options -> options.setURL(url)); }


    @Override
    public <R> DataFrame<R,String> excel(String resource) { return excel(options -> options.setResource(resource)); }

    @Override
    @SuppressWarnings("unchecked")
    public <R> DataFrame<R,String> excel(Consumer<ExcelSourceOptions<R>> configurator) {
        return DataFrameSource.lookup(ExcelSource.class).read(configurator);
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
    @SuppressWarnings("unchecked")
    public <R,C> DataFrame<R,C> json(Consumer<JsonSourceOptions<R,C>> configurator) {
        return DataFrameSource.lookup(JsonSource.class).read(configurator);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> DataFrame<R, String> db(Consumer<DbSourceOptions<R>> configurator) {
        return DataFrameSource.lookup(DbSource.class).read(configurator);
    }
}
