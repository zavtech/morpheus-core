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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedTransferQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.processor.RowProcessor;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameContent;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameSource;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.util.Resource;
import com.zavtech.morpheus.util.http.HttpClient;
import com.zavtech.morpheus.util.text.Formats;
import com.zavtech.morpheus.util.text.parser.Parser;

/**
 * A DataFrameSource designed to load DataFrames from a CSV resource based on the CSV request descriptor.
 *
 * @param <R>   the row key type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class CsvSource<R> extends DataFrameSource<R,String,CsvSourceOptions<R>> {


    /**
     * Constructor
     */
    public CsvSource() {
        super();
    }


    @Override
    public DataFrame<R,String> read(Consumer<CsvSourceOptions<R>> configurator) throws DataFrameException {
        try {
            final CsvSourceOptions<R> options = initOptions(new CsvSourceOptions<>(), configurator);
            final Resource resource = options.getResource();
            switch (resource.getType()) {
                case FILE:          return parse(options, new FileInputStream(resource.asFile()));
                case URL:           return parse(options, resource.asURL());
                case INPUT_STREAM:  return parse(options, resource.asInputStream());
                default:    throw new DataFrameException("Unsupported resource specified in CSVRequest: " + resource);
            }
        } catch (DataFrameException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new DataFrameException("Failed to create DataFrame from CSV source", ex);
        }
    }


    /**
     * Returns a DataFrame parsed from the url specified
     * @param url   the url to parse
     * @return      the DataFrame parsed from url
     * @throws IOException      if there stream read error
     */
    private DataFrame<R,String> parse(CsvSourceOptions<R> request, URL url) throws IOException {
        Objects.requireNonNull(url, "The URL cannot be null");
        if (!url.getProtocol().startsWith("http")) {
            return parse(request, url.openStream());
        } else {
            return HttpClient.getDefault().<DataFrame<R,String>>doGet(httpRequest -> {
                httpRequest.setUrl(url);
                httpRequest.setResponseHandler(response -> {
                    try (InputStream stream = response.getStream()) {
                        final DataFrame<R,String> frame = parse(request, stream);
                        return Optional.ofNullable(frame);
                    } catch (IOException ex) {
                        throw new RuntimeException("Failed to load DataFrame from csv: " + url, ex);
                    }
                });
            }).orElse(null);
        }
    }


    /**
     * Returns a DataFrame parsed from the stream specified stream
     * @param stream    the stream to parse
     * @return          the DataFrame parsed from stream
     * @throws IOException      if there stream read error
     */
    private DataFrame<R,String> parse(CsvSourceOptions<R> options, InputStream stream) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, options.getCharset().orElse(StandardCharsets.UTF_8)))) {
            final CsvRequestHandler handler = new CsvRequestHandler(options);
            final CsvParserSettings settings = new CsvParserSettings();
            settings.getFormat().setDelimiter(options.getDelimiter());
            settings.setHeaderExtractionEnabled(options.isHeader());
            settings.setLineSeparatorDetectionEnabled(true);
            settings.setRowProcessor(handler);
            settings.setIgnoreTrailingWhitespaces(true);
            settings.setIgnoreLeadingWhitespaces(true);
            settings.setMaxColumns(10000);
            settings.setReadInputOnSeparateThread(false);
            final CsvParser parser = new CsvParser(settings);
            parser.parse(reader);
            return handler.getFrame();
        }
    }



    /**
     * A RowProcessor that receives callbacks and incrementally builds the DataFrame.
     */
    private class CsvRequestHandler implements RowProcessor, Runnable {

        private int rowCounter;
        private String[] headers;
        private int[] colIndexes;
        private int logBatchSize;
        private String[] rowValues;
        private volatile boolean done;
        private DataBatch<R> batch;
        private Parser<?>[] parsers;
        private CsvSourceOptions<R> options;
        private DataFrame<R,String> frame;
        private CountDownLatch countDownLatch;
        private Predicate<String[]> rowPredicate;
        private Function<String[],R> rowKeyParser;
        private LinkedTransferQueue<DataBatch<R>> queue;
        private final Object lock = new Object();

        /**
         * Constructor
         * @param options   the options
         */
        CsvRequestHandler(CsvSourceOptions<R> options) {
            this.options = options;
            this.rowPredicate = options.getRowPredicate().orElse(null);
            this.rowKeyParser = options.getRowKeyParser().orElse(null);
            this.logBatchSize = options.getLogBatchSize();
            if (options.isParallel()) {
                this.countDownLatch = new CountDownLatch(1);
                this.queue = new LinkedTransferQueue<>();
                final Thread thread = new Thread(this, "DataFrameCsvReaderThread");
                thread.setDaemon(true);
                thread.start();
            }
        }


        /**
         * Returns the DataFrame result for this processor
         * @return  the DataFrame result
         */
        public DataFrame<R,String> getFrame() {
            try {
                if (options.isParallel()) {
                    this.countDownLatch.await();
                    return frame;
                } else {
                    return frame;
                }
            } catch (Exception ex) {
                throw new DataFrameException("Failed to resolve frame", ex);
            }
        }


        /**
         * Returns true if processing is complete
         * @return  true if processing is complete
         */
        private boolean isComplete() {
            synchronized (lock) {
                return done && queue.isEmpty();
            }
        }


        @Override()
        public void run() {
            try {
                while (!isComplete()) {
                    try {
                        final DataBatch<R> batch = queue.take();
                        if (batch != null && batch.rowCount() > 0) {
                            processBatch(batch);
                        }
                    } catch (Exception ex) {
                        throw new DataFrameException("Failed to process CSV data batch", ex);
                    }
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                countDownLatch.countDown();
            }
        }


        @Override
        public void processStarted(ParsingContext context) {}


        @Override
        public void rowProcessed(String[] row, ParsingContext context) {
            try {
                if (batch == null) {
                    initBatch(row.length, context);
                }
                if (rowPredicate == null || rowPredicate.test(row)) {
                    this.rowCounter++;
                    if (logBatchSize > 0 && rowCounter % logBatchSize == 0) {
                        System.out.println("Loaded " + rowCounter + " rows...");
                    }
                    for (int i = 0; i < colIndexes.length; ++i) {
                        final int colIndex = colIndexes[i];
                        final String rawValue = row.length > colIndex ? row[colIndex] : null;
                        this.rowValues[i] = rawValue;
                    }
                    if (rowKeyParser == null) {
                        this.batch.addRow(rowCounter - 1, rowValues);
                    } else {
                        final R rowKey = rowKeyParser.apply(row);
                        this.batch.addRow(rowKey, rowValues);
                    }
                    if (batch.rowCount() == options.getReadBatchSize()) {
                        if (!options.isParallel()) {
                            this.processBatch(batch);
                            this.batch.clear();
                        } else {
                            synchronized (lock) {
                                this.queue.add(batch);
                                this.batch = new DataBatch<>(options, colIndexes.length);
                                this.lock.notify();
                            }
                        }
                    }
                }
            } catch (DataFrameException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new DataFrameException("Failed to parse row: " + Arrays.toString(row), ex);
            }
        }


        @Override
        public void processEnded(ParsingContext context) {
            try {
                if (!options.isParallel()) {
                    this.batch = batch != null ? batch : new DataBatch<>(options, 0);
                    this.processBatch(batch);
                } else {
                    synchronized (lock) {
                        this.done = true;
                        this.batch = batch != null ? batch : new DataBatch<>(options, 0);
                        this.queue.add(batch);
                    }
                }
            } catch (DataFrameException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new DataFrameException("Failed to process CSV parse end", ex);
            }
        }


        /**
         * Initializes data structures to capture parsed content
         * @param csvColCount   the number of columns in CSV content
         * @param context       the parsing context
         */
        @SuppressWarnings("unchecked")
        private void initBatch(int csvColCount, ParsingContext context) {
            final int colCount = initHeader(csvColCount, context);
            this.rowValues = new String[colCount];
            this.batch = new DataBatch<>(options, colCount);
            this.parsers = new Parser[colCount];
        }


        /**
         * Initializes the header array and column ordinals
         * @param csvColCount   the number of columns in CSV content
         * @param context       the parsing context
         * @return              the column count for frame
         */
        private int initHeader(int csvColCount, ParsingContext context) {
            this.headers = options.isHeader() ? context.headers() : IntStream.range(0, csvColCount).mapToObj(i -> "Column-" + i).toArray(String[]::new);
            this.headers = IntStream.range(0, headers.length).mapToObj(i -> headers[i] != null ? headers[i] : "Column-" + i).toArray(String[]::new);
            this.colIndexes = IntStream.range(0, headers.length).toArray();
            this.options.getColIndexPredicate().ifPresent(predicate -> {
                final Map<String,Integer> indexMap = IntStream.range(0, headers.length).boxed().collect(Collectors.toMap(i -> headers[i], i -> colIndexes[i]));
                this.headers = Arrays.stream(headers).filter(colName -> predicate.test(indexMap.get(colName))).toArray(String[]::new);
                this.colIndexes = Arrays.stream(headers).mapToInt(indexMap::get).toArray();
            });
            this.options.getColNamePredicate().ifPresent(predicate -> {
                final Map<String,Integer> indexMap = IntStream.range(0, headers.length).boxed().collect(Collectors.toMap(i -> headers[i], i -> colIndexes[i]));
                this.headers = Arrays.stream(headers).filter(predicate).toArray(String[]::new);
                this.colIndexes = Arrays.stream(headers).mapToInt(indexMap::get).toArray();
            });
            this.options.getColumnNameMapping().ifPresent(mapping -> {
                final IntStream colOrdinals = IntStream.range(0, headers.length);
                this.headers = colOrdinals.mapToObj(ordinal -> mapping.apply(headers[ordinal], ordinal)).toArray(String[]::new);
            });
            return colIndexes.length;
        }


        /**
         * Initializes the frame based on the contents of the first batch
         * @param batch     the initial batch to initialize frame
         */
        private void initFrame(DataBatch<R> batch) {
            if (headers == null) {
                final Class<R> rowType = options.getRowAxisType();
                final Index<R> rowKeys = Index.of(rowType, 1);
                final Index<String> colKeys = Index.of(String.class, 1);
                this.frame = DataFrame.of(rowKeys, colKeys, Object.class);
            } else {
                final int colCount = headers.length;
                final Formats formats = options.getFormats();
                final Class<R> rowType = options.getRowAxisType();
                final Index<R> rowKeys = Index.of(rowType, options.getRowCapacity().orElse(10000));
                final Index<String> colKeys = Index.of(String.class, colCount);
                this.frame = DataFrame.of(rowKeys, colKeys, Object.class);
                for (int i=0; i<colCount; ++i) {
                    final String colName = headers[i] != null ? headers[i] : "Column-" + i;
                    try {
                        final String[] rawValues = batch.colData(i);
                        final Optional<Parser<?>> userParser = getParser(options.getFormats(), colName);
                        final Optional<Class<?>> colType = getColumnType(colName);
                        if (colType.isPresent()) {
                            final Class<?> type = colType.get();
                            final Parser<?> parser = userParser.orElse(formats.getParserOrFail(type, Object.class));
                            this.parsers[i] = parser;
                            this.frame.cols().add(colName, type);
                        } else {
                            final Parser<?> stringParser = formats.getParserOrFail(String.class);
                            final Parser<?> parser = userParser.orElse(formats.findParser(rawValues).orElse(stringParser));
                            final Set<Class<?>> typeSet = Arrays.stream(rawValues).map(parser).filter(v -> v != null).map(Object::getClass).collect(Collectors.toSet());
                            final Class<?> type = typeSet.size() == 1 ? typeSet.iterator().next() : Object.class;
                            this.parsers[i] = parser;
                            this.frame.cols().add(colName, type);
                        }
                    } catch (Exception ex) {
                        throw new DataFrameException("Failed to inspect seed values in column: " + colName, ex);
                    }
                }
            }
        }

        /**
         * Returns the column type for the column name
         * @param colName   the column name
         * @return          the column type
         */
        private Optional<Class<?>> getColumnType(String colName) {
            final Optional<Class<?>> colType = options.getColumnType(colName);
            if (colType.isPresent()) {
                return colType;
            } else {
                for (Map.Entry<String,Class<?>> entry : options.getColTypeMap().entrySet()) {
                    final String key = entry.getKey();
                    if (colName.matches(key)) {
                        return Optional.of(entry.getValue());
                    }
                }
                return Optional.empty();
            }
        }





        /**
         * Processes the batch of data provided
         * @param batch the batch reference
         */
        private void processBatch(DataBatch<R> batch) {
            int rowIndex = -1;
            try {
                if (frame == null) {
                    initFrame(batch);
                }
                if (batch.rowCount() > 0) {
                    final int rowCount = batch.rowCount();
                    final Array<R> keys = batch.keys();
                    final int fromRowIndex = frame.rowCount();
                    final Array<R> rowKeys = rowCount < options.getReadBatchSize() ? keys.copy(0, rowCount) : keys;
                    this.frame.rows().addAll(rowKeys);
                    final DataFrameContent<R,String> data = frame.data();
                    for (int j=0; j<colIndexes.length; ++j) {
                        final String[] colValues = batch.colData(j);
                        final Parser<?> parser = parsers[j];
                        for (int i=0; i<rowCount; ++i) {
                            rowIndex = fromRowIndex + i;
                            final String rawValue = colValues[i];
                            switch (parser.getStyle()) {
                                case BOOLEAN:   data.setBoolean(rowIndex, j, parser.applyAsBoolean(rawValue));  break;
                                case INTEGER:   data.setInt(rowIndex, j, parser.applyAsInt(rawValue));          break;
                                case LONG:      data.setLong(rowIndex, j, parser.applyAsLong(rawValue));        break;
                                case DOUBLE:    data.setDouble(rowIndex, j, parser.applyAsDouble(rawValue));    break;
                                default:        data.setValue(rowIndex, j, parser.apply(rawValue));             break;
                            }
                        }
                    }
                    if (frame.rowCount() % 100000 == 0) {
                        System.out.println("Processed " + frame.rowCount() + " rows...");
                    }
                }
            } catch (Exception ex) {
                final int lineNo = options.isHeader() ? rowIndex + 2 : rowIndex + 1;
                throw new DataFrameException("Failed to process CSV batch, line no " + lineNo, ex);
            }
        }
    }

    /**
     * Returns the user configured parser for column name
     * @param colName   the column name
     * @return          the parser match
     */
    protected static Optional<Parser<?>> getParser(Formats formats, String colName) {
        final Parser<?> userParser = formats.getParser(colName);
        if (userParser != null) {
            return Optional.of(userParser);
        } else {
            for (Object key : formats.getParserKeys()) {
                if (key instanceof String) {
                    final String keyString = key.toString();
                    if (colName.matches(keyString)) {
                        final Parser<?> parser = formats.getParserOrFail(keyString);
                        return Optional.ofNullable(parser);
                    }
                }
            }
            return Optional.empty();
        }
    }


    /**
     * A class that represents a batch of raw CSV that needs to be parsed into type specific values
     * @param <X>       the row key type
     */
    protected static class DataBatch<X> {

        private Array<X> keys;
        private int rowCount;
        private String[][] data;

        /**
         * Constructor
         * @param request   the CSV request descriptor
         * @param colCount  the column count for this batch
         */
        private DataBatch(CsvSourceOptions<X> request, int colCount) {
            this( request.getRowAxisType(), request.getReadBatchSize(), colCount);
        }

        protected DataBatch(Class<X> rowAxisType, int readBatchSize, int colCount) {
            this.keys = Array.of(rowAxisType, readBatchSize);
            this.data = new String[colCount][readBatchSize];
        }

        /**
         * Returns the row count for this batch
         * @return  the populated row count
         */
        protected int rowCount() {
            return rowCount;
        }

        /**
         * Returns the keys for this batch
         * @return  the keys for this batch
         */
        protected Array<X> keys() {
            return keys;
        }

        /**
         * Returns the vector of column data for the index
         * @param colIndex  the column index
         * @return          the column vector
         */
        protected String[] colData(int colIndex) {
            return data[colIndex];
        }

        /**
         * Resets this batch so that it can be used again
         */
        protected void clear() {
            this.rowCount = 0;
            this.keys.fill(null);
            for (int i=0; i<data.length; ++i) {
                for (int j=0; j<data[i].length; j++) {
                    this.data[i][j] = null;
                }
            }
        }

        /**
         * Adds a row to this batch with the key provided
         * @param rowKey    the row key
         * @param rowValues the row value tokens
         * @return          the row index in batch
         */
        protected int addRow(X rowKey, String[] rowValues) {
            this.keys.setValue(rowCount, rowKey);
            for (int i=0; i<rowValues.length; ++i) {
                this.data[i][rowCount] = rowValues[i];
            }
            return rowCount++;
        }

        /**
         * Adds a row to this batch with the key provided
         * @param rowKey    the row key
         * @param rowValues the row value tokens
         * @return          the row index in batch
         */
        protected int addRow(int rowKey, String[] rowValues) {
            this.keys.setInt(rowCount, rowKey);
            for (int i=0; i<rowValues.length; ++i) {
                this.data[i][rowCount] = rowValues[i];
            }
            return rowCount++;
        }

    }


    public static void main(String[] args) {
        final long t1 = System.currentTimeMillis();
        final String path = "/Users/witdxav/Dropbox/data/uk-house-prices/uk-house-prices-2006.csv";
        final DataFrame<Integer,String> frame = DataFrame.read().csv(options -> {
            options.setResource(path);
            options.setHeader(false);
            options.setParallel(true);
        });
        final long t2 = System.currentTimeMillis();
        frame.out().print();
        System.out.printf("\n\nLoaded DataFrame with %s row in %s millis", frame.rowCount(), t2 - t1);
    }

}
