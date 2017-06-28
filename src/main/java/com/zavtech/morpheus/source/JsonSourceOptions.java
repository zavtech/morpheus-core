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
package com.zavtech.morpheus.source;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.util.Asserts;
import com.zavtech.morpheus.util.Resource;
import com.zavtech.morpheus.util.text.Formats;

/**
 * A DataFrameRequest used to load a DataFrame from a Resource expressed in Morpheus JSON format.
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class JsonSourceOptions<R,C> implements DataFrameSource.Options<R,C> {

    private Resource resource;
    private Formats formats;
    private Charset charset;
    private Predicate<R> rowPredicate;
    private Predicate<C> colPredicate;

    /**
     * Constructor
     */
    public JsonSourceOptions() {
        this.formats = new Formats();
        this.charset = StandardCharsets.UTF_8;
    }


    @Override
    public void validate() {
        Asserts.notNull(this.getResource(), "The JSON options resource cannot be null");
        Asserts.notNull(this.getCharset(), "The JSON options charset cannot be null");
        Asserts.notNull(this.getFormats(), "The JSON options formats cannot be null");
    }

    /**
     * Returns the resource for this request
     * @return  the resource
     */
    public Resource getResource() {
        return resource;
    }

    /**
     * Returns the formats with decoders to parse JSON
     * @return  the formats with decoders
     */
    public Formats getFormats() {
        return formats != null ? formats : new Formats();
    }

    /**
     * Returns the charset encoding
     * @return  the charset for encoding
     */
    public Charset getCharset() {
        return charset != null? charset : StandardCharsets.UTF_8;
    }

    /**
     * Returns the row predicate for this request
     * @return      the row predicate
     */
    public Optional<Predicate<R>> getRowPredicate() {
        return Optional.ofNullable(rowPredicate);
    }

    /**
     * Returns the column predicate for this request
     * @return  the column predicate
     */
    public Optional<Predicate<C>> getColPredicate() {
        return Optional.ofNullable(colPredicate);
    }

    /**
     * Sets the input file for these options
     * @param file  the input file
     */
    public void setFile(File file) {
        this.resource = Resource.of(file);
    }

    /**
     * Sets the input URL for these options
     * @param url   the input url
     */
    public void setURL(URL url) {
        this.resource = Resource.of(url);
    }

    /**
     * Applies to resource to load CSV content from
     * @param inputStream   the input stream to load from
     */
    public void setInputStream(InputStream inputStream) {
        Objects.requireNonNull(inputStream, "The resource input srream cannot be null");
        this.resource = Resource.of(inputStream);
    }

    /**
     * Applies to resource to load CSV content from
     * @param resource  the resource to load from (file, URL or Classpath resource)
     */
    public void setResource(String resource) {
        Objects.requireNonNull(resource, "The resource cannot be null");
        this.resource = Resource.of(resource);
    }

    /**
     * Applies to resource to load CSV content from
     * @param resource  the resource to load from (file, URL or Classpath resource)
     */
    public void setResource(Resource resource) {
        Objects.requireNonNull(resource, "The resource cannot be null");
        this.resource = resource;
    }

    /**
     * Sets the charset encoding for resource
     * @param charset   the charset encoding
     */
    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    /**
     * Applies formats with relevant decoders for CSV parsing
     * @param formats   the formats for parsing
     */
    public void setFormats(Formats formats) {
        this.formats = formats;
    }

    /**
     * Sets the row predicate for this request
     * @param rowPredicate  the row predicate, null permitted
     */
    public void setRowPredicate(Predicate<R> rowPredicate) {
        this.rowPredicate = rowPredicate;
    }

    /**
     * Sets the column predicate for this request
     * @param colPredicate  the column predicate, null permitted
     */
    public void setColPredicate(Predicate<C> colPredicate) {
        this.colPredicate = colPredicate;
    }
}
