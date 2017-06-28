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
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.function.Consumer;

import com.zavtech.morpheus.util.text.Formats;

/**
 * An interface that provides various functions for writing a DataFrame to an output device in various formats.
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameOutput<R,C> {

    /**
     * Prints at most the first 10 rows of the DataFrame to standard out
     * @throws DataFrameException  if there is an IO exception
     */
    void print();

    /**
     * Prints the contents of this DataFrame to standard output
     * @param maxRows       the max number of rows to print
     * @throws DataFrameException  if there is an IO exception
     */
    void print(int maxRows);

    /**
     * Prints the contents of this DataFrame to the PrintStream provided
     * @param maxRows       the max number of rows to print
     * @param stream        the stream ro print to
     * @throws DataFrameException  if there is an IO exception
     */
    void print(int maxRows, OutputStream stream);

    /**
     * Prints at most the first 10 rows of the DataFrame to standard out
     * @param formatting    consumer to configure formatting
     * @throws DataFrameException  if there is an IO exception
     */
    void print(Consumer<Formats> formatting);

    /**
     * Prints the contents of this DataFrame to standard output
     * @param maxRows       the max number of rows to print
     * @param formatting    consumer to configure formatting
     * @throws DataFrameException  if there is an IO exception
     */
    void print(int maxRows, Consumer<Formats> formatting);

    /**
     * Prints the contents of this DataFrame to the PrintStream provided
     * @param maxRows       the max number of rows to print
     * @param stream        the stream ro print to
     * @param formatting    consumer to configure formatting
     * @throws DataFrameException  if there is an IO exception
     */
    void print(int maxRows, OutputStream stream, Consumer<Formats> formatting);

    /**
     * Writes the contents of the DataFrame as CSV to the file specified
     * @param file  the file to write to
     * @throws DataFrameException  if there is an I/O exception
     */
    void writeCsv(File file) throws DataFrameException;

    /**
     * Writes the contents of the DataFrame as CSV to the output stream specified
     * @param os    the output stream to write frame to
     * @throws DataFrameException  if there is an I/O exception
     */
    void writeCsv(OutputStream os) throws DataFrameException;

    /**
     * Writes the contents of the DataFrame as CSV to the file specified
     * @param file      the file to write to
     * @param formats   the formats specification
     * @throws DataFrameException  if there is an I/O exception
     */
    void writeCsv(File file, Formats formats) throws DataFrameException;

    /**
     * Writes the contents of the DataFrame as CSV to the output stream specified
     * @param os        the output stream to write frame to
     * @param formats   the formats specification
     * @throws DataFrameException  if there is an I/O exception
     */
    void writeCsv(OutputStream os, Formats formats) throws DataFrameException;

    /**
     * Writes the contents of the DataFrame as JSON to the file specified
     * @param file  the file to write to
     * @throws DataFrameException  if there is an I/O exception
     */
    void writeJson(File file) throws DataFrameException;

    /**
     * Writes the contents of the DataFrame as JSON to the output stream specified
     * @param os    the output stream to write frame to
     * @throws DataFrameException  if there is an I/O exception
     */
    void writeJson(OutputStream os) throws DataFrameException;

    /**
     * Writes the contents of the DataFrame as JSON to the file specified
     * @param file      the file to write to
     * @param formats   the formats specification
     * @throws DataFrameException  if there is an I/O exception
     */
    void writeJson(File file, Formats formats) throws DataFrameException;

    /**
     * Writes the contents of the DataFrame as JSON to the output stream specified
     * @param os        the output stream to write frame to
     * @param formats   the formats specification
     * @throws DataFrameException  if there is an I/O exception
     */
    void writeJson(OutputStream os, Formats formats) throws DataFrameException;



}
