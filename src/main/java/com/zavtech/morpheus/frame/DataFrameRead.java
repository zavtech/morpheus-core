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

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.function.Consumer;

import com.zavtech.morpheus.source.CsvSourceOptions;
import com.zavtech.morpheus.source.DbSourceOptions;
import com.zavtech.morpheus.source.ExcelSourceOptions;
import com.zavtech.morpheus.source.JsonSourceOptions;

/**
 * An interface used to read a DataFrame stored in various formats from some underlying storage devices.
 *
 * <p>This is open source software released under the <a href="http://www.ap`ache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameRead {

    /**
     * Reads a DataFrame from a CSV file
     * @param file      the input file
     * @param <R>       the row key type
     * @return          the resulting DataFrame
     */
    <R> DataFrame<R,String> csv(File file);

    /**
     * Reads a DataFrame from a CSV file
     * @param url       the input url
     * @param <R>       the row key type
     * @return          the resulting DataFrame
     */
    <R> DataFrame<R,String> csv(URL url);

    /**
     * Reads a DataFrame from a CSV file
     * @param is        the input stream to read from
     * @param <R>       the row key type
     * @return          the resulting DataFrame
     */
    <R> DataFrame<R,String> csv(InputStream is);

    /**
     * Reads a DataFrame from a CSV resource
     * @param resource      a file name or URL
     * @param <R>           the row key type
     * @return              the resulting DataFrame
     */
    <R> DataFrame<R,String> csv(String resource);

    /**
     * Reads a DataFrame from a CSV resource based on the options configurator
     * @param configurator  the configurator for CSV options
     * @param <R>           the row key type
     * @return              the resulting DataFrame
     */
    <R> DataFrame<R,String> csv(Consumer<CsvSourceOptions<R>> configurator);

    /**
     * Reads a DataFrame from a excel InputStream
     * @param is        the input stream to read from
     * @param <R>       the row key type
     * @return          the resulting DataFrame
     */
    <R> DataFrame<R,String> excel(InputStream is);

    /**
     * Reads a DataFrame from a url based on the options configurator
     * @param url      a filename or URL
     * @param <R>           the row key type
     * @return              the resulting DataFrame
     */
    <R> DataFrame<R,String> excel(URL url);

    /**
     * Reads a DataFrame from a Excel resource based on the options configurator
     * @param resource      a filename or URL
     * @param <R>           the row key type
     * @return              the resulting DataFrame
     */
    <R> DataFrame<R,String> excel(String resource);

    /**
     * Reads a DataFrame from a Excel resource based on the options configurator
     * @param configurator  the configurator for Excel options
     * @param <R>           the row key type
     * @return              the resulting DataFrame
     */
    <R> DataFrame<R,String> excel(Consumer<ExcelSourceOptions<R>> configurator);

    /**
     * Reads a DataFrame from a JSON file
     * @param file      the input file
     * @param <R>       the row key type
     * @param <C>       the column key type
     * @return          the resulting DataFrame
     */
    <R,C> DataFrame<R,C> json(File file);

    /**
     * Reads a DataFrame from a JSON file
     * @param url       the input url
     * @param <R>       the row key type
     * @param <C>       the column key type
     * @return          the resulting DataFrame
     */
    <R,C> DataFrame<R,C> json(URL url);

    /**
     * Reads a DataFrame from a JSON file
     * @param is        the input stream to read from
     * @param <R>       the row key type
     * @param <C>       the column key type
     * @return          the resulting DataFrame
     */
    <R,C> DataFrame<R,C> json(InputStream is);

    /**
     * Reads a DataFrame from a JSON resource
     * @param resource      a file name or URL
     * @param <R>           the row key type
     * @param <C>           the column key type
     * @return              the resulting DataFrame
     */
    <R,C> DataFrame<R,C> json(String resource);

    /**
     * Reads a DataFrame from a JSON resource based on the options configurator
     * @param configurator  the configurator for JSON options
     * @param <R>           the row key type
     * @param <C>           the column key type
     * @return              the resulting DataFrame
     */
    <R,C> DataFrame<R,C> json(Consumer<JsonSourceOptions<R,C>> configurator);

    /**
     * Reads a DataFrame from a database based on the options configurator
     * @param configurator  the configurator for DB options
     * @param <R>           the row key type
     * @return              the resulting DataFrame
     */
    <R> DataFrame<R,String> db(Consumer<DbSourceOptions<R>> configurator);

}
