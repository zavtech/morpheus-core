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

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameCursor;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.util.text.Formats;
import com.zavtech.morpheus.util.text.printer.Printer;

/**
 * A class that can pretty print a DataFrame to text for visualization in a console
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class XDataFramePrinter {

    private int maxRows;
    private Formats formats;
    private OutputStream stream;

    /**
     * Constructor
     * @param maxRows   the max rows to print
     * @param formats   the formats for this printer
     * @param stream    the print stream to write to
     */
    XDataFramePrinter(int maxRows, Formats formats, OutputStream stream) {
        this.formats = formats;
        this.maxRows = maxRows;
        this.stream = stream;
    }

    /**
     * Prints the specified DataFrame to the stream bound to this printer
     * @param frame the DataFrame to print
     */
    void print(DataFrame<?,?> frame) {
        try {
            final String[] headers = getHeaderTokens(frame);
            final String[][] data = getDataTokens(frame);
            final int[] widths = getWidths(headers, data);
            final String dataTemplate = getDataTemplate(widths);
            final String headerTemplate = getHeaderTemplate(widths, headers);
            final int totalWidth = IntStream.of(widths).map(w -> w + 5).sum()-1;
            final StringBuilder text = new StringBuilder(totalWidth * data.length).append("\n");
            final String headerLine = String.format(headerTemplate, headers);
            text.append(headerLine).append("\n");
            for (int j=0; j<totalWidth; ++j) {
                text.append("-");
            }
            for (String[] row : data) {
                final String dataLine = String.format(dataTemplate, row);
                text.append("\n");
                text.append(dataLine);
            }
            final byte[] bytes = text.toString().getBytes();
            this.stream.write(bytes);
            this.stream.flush();
        } catch (IOException ex) {
            throw new DataFrameException("Failed to print DataFrame", ex);
        }
    }


    /**
     * Returns the header string tokens for the frame
     * @param frame     the frame to create header tokens
     * @return          the header tokens
     */
    private String[] getHeaderTokens(DataFrame<?,?> frame) {
        final int colCount = frame.colCount() + 1;
        final String[] header = new String[colCount];
        final Class<?> columnKeyType = frame.cols().keyType();
        final Printer<Object> printer = formats.getPrinterOrFail(columnKeyType, Object.class);
        IntStream.range(0, colCount).forEach(colIndex -> {
            if (colIndex == 0) header[colIndex] = "Index";
            else {
                final Object colKey = frame.cols().key(colIndex - 1);
                final String colText = printer.apply(colKey);
                header[colIndex] = colText;
            }
        });
        return header;
    }


    /**
     * Returns the 2-D array of data tokens from the frame specified
     * @param frame     the DataFrame from which to create 2D array of formatted tokens
     * @return          the array of data tokend
     */
    private String[][] getDataTokens(DataFrame<?,?> frame) {
        if (frame.rowCount() == 0) return new String[0][0];
        final int rowCount = Math.min(maxRows, frame.rowCount());
        final int colCount = frame.colCount() + 1;
        final Printer<Object> rowKeyPrinter = formats.getPrinterOrFail(frame.rows().keyType(), Object.class);
        final List<Printer<Object>> printers = getColumnPrinters(frame);
        final String[][] data = new String[rowCount][colCount];
        final DataFrameCursor<?,?> cursor = frame.cursor().moveTo(0, 0);
        for (int i=0; i<rowCount; ++i) {
            cursor.moveToRow(i);
            for (int j=0; j<=frame.colCount(); ++j) {
                try {
                    if (j == 0) {
                        final Object rowKey = cursor.rowKey();
                        final String rowText = rowKeyPrinter.apply(rowKey);
                        data[i][j] = rowText;
                    } else {
                        cursor.moveToColumn(j-1);
                        final Printer<Object> printer = printers.get(j -1);
                        final Object value = cursor.getValue();
                        final String text =  printer.apply(value);
                        data[i][j] = text;
                    }
                } catch (Exception ex) {
                    final String coordinates = String.format("(%s, %s)", frame.rows().key(i), frame.cols().key(j-1));
                    throw new DataFrameException("Failed to print value at: " + coordinates, ex);
                }
            }
        }
        return data;
    }


    /**
     * Returns the list of printers for each column in the frame
     * @param frame     the frame reference
     * @return          the list of printers
     */
    private List<Printer<Object>> getColumnPrinters(DataFrame<?,?> frame) {
        final Printer<Object> objectPrinter = formats.getPrinterOrFail(Object.class);
        return frame.cols().stream().map(col -> {
            final Object colKey = col.key();
            final Class<?> colType = col.typeInfo();
            final Printer<Object> printer0 = formats.getPrinter(colKey);
            final Printer<Object> printer1 = formats.getPrinter(colType);
            final Printer<Object> printer = printer0 != null ? printer0 : printer1;
            return printer != null ? printer : objectPrinter;
        }).collect(Collectors.toList());
    }


    /**
     * Returns the column widths required to print the header and data
     * @param headers   the headers to print
     * @param data      the data items to print
     * @return          the required column widths
     */
    private static int[] getWidths(String[] headers, String[][] data) {
        final int[] widths = new int[headers.length];
        for (int j=0; j<headers.length; ++j) {
            final String header = headers[j];
            widths[j] = Math.max(widths[j], header != null ? header.length() : 0);
        }
        for (String[] rowValues : data) {
            for (int j=0; j<rowValues.length; ++j) {
                final String value = rowValues[j];
                widths[j] = Math.max(widths[j], value != null ? value.length() : 0);
            }
        }
        return widths;
    }


    /**
     * Returns the header template given the widths specified
     * @param widths    the token widths
     * @return          the line format template
     */
    private static String getHeaderTemplate(int[] widths, String[] headers) {
        return IntStream.range(0, widths.length).mapToObj(i -> {
            final int width = widths[i];
            final int length = headers[i].length();
            final int leading = (width - length) / 2;
            final int trailing = width - (length + leading);
            final StringBuilder text = new StringBuilder();
            whitespace(text, leading + 1);
            text.append("%").append(i+1).append("$s");
            whitespace(text, trailing);
            text.append("  |");
            return text.toString();
        }).reduce((left, right) -> left + " " + right).orElseGet(() -> "");
    }

    /**
     * Returns the data template given the widths specified
     * @param widths    the token widths
     * @return          the line format template
     */
    private static String getDataTemplate(int[] widths) {
        return IntStream.range(0, widths.length)
                .mapToObj(i -> " %" + (i+1) + "$" + widths[i] + "s  |")
                .reduce((left, right) -> left + " " + right)
                .orElseGet(() -> "");
    }

    /**
     * Returns a whitespace string of the length specified
     * @param length    the length for whitespace
     */
    private static void whitespace(StringBuilder text, int length) {
        IntStream.range(0, length).forEach(i -> text.append(" "));
    }

}
