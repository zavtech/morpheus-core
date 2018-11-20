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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.util.Asserts;
import com.zavtech.morpheus.util.Predicates;
import com.zavtech.morpheus.util.Resource;
import com.zavtech.morpheus.util.functions.ObjectIntBiFunction;
import com.zavtech.morpheus.util.text.Formats;
import com.zavtech.morpheus.util.text.parser.Parser;

/**
 * A DataFrameRequest used to load a DataFrame from a Resource expressed in ASCII CSV format.
 *
 * @param <R> the row key type <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 * @author Xavier Witdouck
 */
public class CsvSourceOptions<R> implements DataFrameSource.Options<R,String> {

    private boolean header;
    private Formats formats;
    private Resource resource;
    private boolean parallel;
    private Charset charset;
    private int rowCapacity;
    private Class<R> rowAxisType;
    private int logBatchSize;
    private int readBatchSize = 1000;
    private char delimiter = ',';
    private Predicate<String[]> rowPredicate;
    private Function<String[],R> rowKeyParser;
    private Predicate<String> colNamePredicate;
    private Predicate<Integer> colIndexPredicate;
    private Map<String,Class<?>> colTypeMap = new HashMap<>();
    private ObjectIntBiFunction<String,String> columnNameMapping;
    private int maxColumns;

    /**
     * Constructor
     */
    @SuppressWarnings("unchecked")
    public CsvSourceOptions() {
        this.header = true;
        this.rowCapacity = 1000;
        this.readBatchSize = 1000;
        this.maxColumns = 10_000;
        this.formats = new Formats();
        this.rowAxisType = (Class<R>)Integer.class;
        this.charset = StandardCharsets.UTF_8;
        this.delimiter = ',';
    }

    @Override
    public void validate() {
        Asserts.notNull(getResource(), "The CSV options resource cannot be null");
        Asserts.notNull(getFormats(), "The CSV formats cannot be null");
        Asserts.notNull(getRowAxisType(), "The CSV row axis type cannot be null");
        Asserts.notNull(getCharset(), "The CSV charset cannot be null");
    }

    /**
     * Returns the row axis type
     * @return  the row axis type
     */
    public Class<R> getRowAxisType() {
        return rowAxisType;
    }

    /**
     * Returns true if first row should be parsed as header
     * @return  true to parse first row as header
     */
    public boolean isHeader() {
        return header;
    }

    /**
     * Returns the type formats
     * @return  the type formats
     */
    public Formats getFormats() {
        return formats;
    }

    /**
     * Returns the resource for this request
     * @return  the resource for request
     */
    public Resource getResource() {
        return resource;
    }

    /**
     * Returns the delimiter used to split lines
     * @return      the delimiter to split lines
     */
    public char getDelimiter() {
        return delimiter;
    }

    /**
     * Returns true for parallel processing
     * @return  true for parallel processing
     */
    public boolean isParallel() {
        return parallel;
    }

    /**
     * Returns the row batch size for processing
     * @return  the row batch size, which is also used to size the seed for type resolution
     */
    public int getReadBatchSize() {
        return readBatchSize;
    }

    /**
     * Returns the log batch size for printing progress to std out
     * @return  the log batch size for printing progress to std out
     */
    public int getLogBatchSize() {
        return logBatchSize;
    }

    /**
     * Returns the charset for encoding
     * @return  the charset encoding
     */
    public Optional<Charset> getCharset() {
        return Optional.ofNullable(charset);
    }

    /**
     * Returns the optional row capacity for this request
     * @return      the row capacity for request
     */
    public Optional<Integer> getRowCapacity() {
        return Optional.ofNullable(rowCapacity);
    }


    /**
     * Returns the optional maximal columns for this request
     * @return      the maximal columns for request
     */
    public Optional<Integer> getMaxColumns() {
        return Optional.ofNullable(maxColumns);
    }

    /**
     * Returns the optional column type for the column name
     * @param colName   the column name
     * @return          the optional column type
     */
    public Optional<Class<?>> getColumnType(String colName) {
        return Optional.ofNullable(colTypeMap.get(colName));
    }

    /**
     * Returns the optional column name predicate for this request
     * @return  the optional column name predicate
     */
    public Optional<Predicate<String>> getColNamePredicate() {
        return Optional.ofNullable(colNamePredicate);
    }

    /**
     * Returns the optional column index predicate for this request
     * @return  the optional column index predicate
     */
    public Optional<Predicate<Integer>> getColIndexPredicate() {
        return Optional.ofNullable(colIndexPredicate);
    }

    /**
     * Returns the optional row predicate for this request
     * @return      the optional row predicate
     */
    public Optional<Predicate<String[]>> getRowPredicate() {
        return Optional.ofNullable(rowPredicate);
    }

    /**
     * Returns the row key function for this request
     * @return      the row key function for request
     */
    public  Optional<Function<String[],R>> getRowKeyParser() {
        return Optional.ofNullable(rowKeyParser);
    }

    /**
     * Returns the apply of column types for this request
     * @return      the apply of column types keyed by column name regex
     */
    public final Map<String,Class<?>> getColTypeMap() {
        return Collections.unmodifiableMap(colTypeMap);
    }

    /**
     * Returns the column mapping function used to rename columns
     * @return      the column mapping function
     */
    public Optional<ObjectIntBiFunction<String,String>> getColumnNameMapping() {
        return Optional.ofNullable(columnNameMapping);
    }

    /**
     * Sets the input file for these options
     * @param file  the input file
     */
    public void setFile(File file) {
        Objects.requireNonNull(file, "The file cannot be null");
        this.resource = Resource.of(file);
    }

    /**
     * Sets the input url for these options
     * @param url   the input url
     */
    public void setURL(URL url) {
        Objects.requireNonNull(url, "The file cannot be null");
        this.resource = Resource.of(url);
    }

    /**
     * Sets the input stream for these options
     * @param is    the input stream
     */
    public void setInputStream(InputStream is) {
        Objects.requireNonNull(is, "The InputStream resource cannot be null");
        this.resource = Resource.of(is);
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
    public final void setResource(Resource resource) {
        Objects.requireNonNull(resource, "The resource cannot be null");
        this.resource = resource;
    }

    /**
     * Applies to resource to load CSV content from
     * @param inputStream   the input stream to load from
     */
    public void setResource(InputStream inputStream) {
        Objects.requireNonNull(inputStream, "The resource input stream cannot be null");
        this.resource = Resource.of(inputStream);
    }

    /**
     * Sets the delimiter used to tokenize a line
     * @param delimiter the delimiter for line
     */
    public void setDelimiter(char delimiter) {
        this.delimiter = delimiter;
    }

    /**
     * Applies the row axis key type and associated parser function
     * @param rowType           the row axis key type
     * @param parser    the function that generates a key given tokens for a row
     */
    public void setRowKeyParser(Class<R> rowType, Function<String[],R> parser) {
        Asserts.notNull(rowType, "The row key type cannot be null");
        Asserts.notNull(parser, "The row key function cannot be null");
        this.rowAxisType = rowType;
        this.rowKeyParser = parser;
    }

    /**
     * Applies the character set encoding for this request
     * @param charset   the character set encoding
     */
    public void setCharset(Charset charset) {
        Objects.requireNonNull(charset, "The charset cannot be null");
        this.charset = charset;
    }

    /**
     * Applies the header attribute to this request
     * @param header    true if first row should be parsed as header
     */
    public void setHeader(boolean header) {
        this.header = header;
    }

    /**
     * Applies formats with relevant decoders for CSV parsing
     * @param formats   the formats for parsing
     */
    public void setFormats(Formats formats) {
        this.formats = formats;
    }

    /**
     * Sets whether to parallel process CSV content
     * @param parallel  true for parallel processing
     */
    public void setParallel(boolean parallel) {
        this.parallel = parallel;
    }

    /**
     * Sets the row batch size for processing, which is also used to size the seed for type resolution
     * @param readBatchSize the row batch size, which is also used to size the seed for type resolution
     */
    public void setReadBatchSize(int readBatchSize) {
        this.readBatchSize = readBatchSize;
    }

    /**
     * Sets the log batch size for reporting progress to std out
     * @param logBatchSize  the log batch size for reporting read progress
     */
    public void setLogBatchSize(int logBatchSize) {
        this.logBatchSize = logBatchSize;
    }

    /**
     * Sets the row capacity for this request
     * @param rowCapacity   the row capacity
     */
    public void setRowCapacity(int rowCapacity) {
        this.rowCapacity = rowCapacity;
    }

    /**
     * Sets the max columns to parse for this request
     * @param maxColumns   the max columns
     */
    public void setMaxColumns(int maxColumns) {
        this.maxColumns = maxColumns;
    }

    /**
     * Applies the row predicate to select subset of rows
     * @param predicate the row predicate to load a subset of rows
     */
    public void setRowPredicate(Predicate<String[]> predicate) {
        this.rowPredicate = predicate;
    }

    /**
     * Applies the column name predicate to select a subset of columns
     * @param colNamePredicate  the column predicate
     */
    public void setColNamePredicate(Predicate<String> colNamePredicate) {
        this.colNamePredicate = colNamePredicate;
    }

    /**
     * Applies the column index predicate to select a subset of columns
     * @param colIndexPredicate the column index predicate
     */
    public void setColIndexPredicate(Predicate<Integer> colIndexPredicate) {
        this.colIndexPredicate = colIndexPredicate;
    }

    /**
     * Applies a column predicate that includes the specified columns
     * @param columns   the column names to include
     */
    public void setIncludeColumns(String... columns) {
        this.setColNamePredicate(Predicates.in(columns));
    }

    /**
     * Applies a column predicate that excludes the specified columns
     * @param columns   the column names to exclude
     */
    public void setExcludeColumns(String... columns) {
        this.setColNamePredicate(Predicates.in(columns).negate());
    }

    /**
     * Applies a column index predicate to include the specified indexes
     * @param columns   the column indexes to include
     */
    public void setIncludeColumnIndexes(int... columns) {
        this.setColIndexPredicate(Predicates.in(columns));
    }

    /**
     * Applies a column index predicate to exclude the specified indexes
     * @param columns   the column indexes to exclude
     */
    public void setExcludeColumnIndexes(int... columns) {
        this.setColIndexPredicate(Predicates.in(columns).negate());
    }

    /**
     * Applies the column type for the name specified
     * @param colNameRegex  the column name, which can be a regular expression
     * @param type          the column data type
     */
    public void setColumnType(String colNameRegex, Class<?> type) {
        this.colTypeMap.put(colNameRegex, type);
        if (formats != null && formats.getParser(colNameRegex) == null) {
            final Parser<?> parser = formats.getParser(type);
            if (parser != null) {
                this.formats.setParser(colNameRegex, parser);
            }
        }
    }

    /**
     * Applies a parser function for the column name and type specified
     * @param colNameRegex  the column name, which can be a regular expression
     * @param parser        the parser function
     * @param <T>           the type
     */
    public <T> void setParser(String colNameRegex, Parser<T> parser) {
        this.formats.setParser(colNameRegex, parser);
    }

    /**
     * Applies a parser function for the column name and type specified
     * @param colNameRegex  the column name, which can be a regular expression
     * @param type          the type for which to apply the default Parser
     */
    public void setParser(String colNameRegex, Class<?> type) {
        this.formats.setParser(colNameRegex, type);
    }

    /**
     * Applies a column mapping function that can be used to rename columns
     * @param columnNameMapping     the column mapping function, null permitted
     */
    public void setColumnNameMapping(ObjectIntBiFunction<String,String> columnNameMapping) {
        this.columnNameMapping = columnNameMapping;
    }

    /**
     * Sets the formats configurator for these options
     * @param configurator  the formats configuratpr
     */
    public void setFormats(Consumer<Formats> configurator) {
        this.formats = this.formats != null ? this.formats : new Formats();
        configurator.accept(formats);
    }

}
