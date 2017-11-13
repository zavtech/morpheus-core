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
package com.zavtech.morpheus.sink;

import java.io.OutputStream;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameContent;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameSink;
import com.zavtech.morpheus.util.Initialiser;
import com.zavtech.morpheus.util.text.Formats;
import com.zavtech.morpheus.util.text.printer.Printer;

/**
 * A DataFrameSink implementation that writes a DataFrame to some output device in CSV format.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class CsvSink<R,C> implements DataFrameSink<R,C,CsvSinkOptions<R>> {

    /**
     * Constructor
     */
    public CsvSink() {
        super();
    }

    @Override()
    @SuppressWarnings("unchecked")
    public void write(DataFrame<R,C> frame, Consumer<CsvSinkOptions<R>> configurator) {
        final CsvSinkOptions<R> options = Initialiser.apply(new CsvSinkOptions<>(), configurator);
        Objects.requireNonNull(options.getFormats(), "The CSV options formats cannot be null");
        Objects.requireNonNull(options.getResource(), "The CSV options output resource cannot be null");
        Objects.requireNonNull(options.getSeparator(), "The CSV options separator cannot be null");
        try (OutputStream os = options.getResource().toOutputStream()) {
            final Formats formats = options.getFormats();
            final String separator = options.getSeparator();
            final String nullText = options.getNullText();
            if (options.isIncludeColumnHeader()) {
                writeHeader(frame, options, os);
            }
            final Class<R> rowKeyType = frame.rows().keyType();
            final DataFrameContent<R,C> data = frame.data();
            final Printer<R> rowKeyPrinter = options.getRowKeyPrinter().orElse(formats.getPrinterOrFail(rowKeyType));
            final List<Printer<?>> colPrinters = frame.cols().stream().map(c -> formats.getPrinterOrFail(c.key(), c.typeInfo())).collect(Collectors.toList());
            for (int i = 0; i < frame.rowCount(); ++i) {
                final StringBuilder row = new StringBuilder();
                if (options.isIncludeRowHeader()) {
                    final R rowKey = frame.rows().key(i);
                    row.append(rowKeyPrinter.apply(rowKey));
                    row.append(separator);
                }
                for (int j = 0; j < frame.colCount(); ++j) {
                    final Printer<?> printer = colPrinters.get(j);
                    switch (printer.getStyle()) {
                        case BOOLEAN:   row.append(printer.apply(data.getBoolean(i, j)));   break;
                        case INTEGER:   row.append(printer.apply(data.getInt(i, j)));       break;
                        case LONG:      row.append(printer.apply(data.getLong(i, j)));      break;
                        case DOUBLE:    row.append(printer.apply(data.getDouble(i, j)));    break;
                        default:        row.append(printer.apply(data.getValue(i, j)));     break;
                    }
                    if (j < frame.colCount() - 1) {
                        row.append(separator);
                    } else {
                        row.append("\n");
                        os.write(row.toString().getBytes());
                    }
                }
            }
        } catch (DataFrameException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new DataFrameException("Failed to write DataFrame to CSV output", ex);
        }
    }

    /**
     * Writes the frame column header to the output stream
     * @param frame     the frame to write headers for
     * @param options   the options to tailor output
     * @param os        the output stream to write to
     * @throws DataFrameException  if there is a write error
     */
    private void writeHeader(DataFrame<R,C> frame, CsvSinkOptions options, OutputStream os) {
        try {
            final StringBuilder header = new StringBuilder();
            if (options.isIncludeRowHeader()) {
                header.append(options.getTitle());
                header.append(options.getSeparator());
            }
            final Formats formats = options.getFormats();
            final Printer<C> printer = formats.getPrinterOrFail(frame.cols().keyType());
            for (int i = 0; i<frame.colCount(); ++i) {
                final C column = frame.cols().key(i);
                header.append(printer.apply(column));
                if (i<frame.colCount()-1) {
                    header.append(options.getSeparator());
                } else {
                    header.append("\n");
                    os.write(header.toString().getBytes());
                }
            }
        } catch (Exception ex) {
            throw new DataFrameException("Failed to write DataFrame header to CSV", ex);
        }
    }

}
