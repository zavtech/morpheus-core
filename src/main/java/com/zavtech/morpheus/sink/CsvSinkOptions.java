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

import java.io.File;
import java.io.OutputStream;
import java.util.Optional;
import java.util.function.Consumer;

import com.zavtech.morpheus.util.Resource;
import com.zavtech.morpheus.util.text.Formats;
import com.zavtech.morpheus.util.text.printer.Printer;

/**
 * A class that defines the various options that can be used to control the output of a DataFrame to CSV
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class CsvSinkOptions<R> {

    private String title;
    private String nullText;
    private Formats formats;
    private String separator;
    private Resource resource;
    private boolean includeRowHeader;
    private boolean includeColumnHeader;
    private Printer<R> rowKeyPrinter;

    /**
     * Constructor
     */
    CsvSinkOptions() {
        this.separator = ",";
        this.title = "DataFrame";
        this.formats = new Formats();
        this.includeRowHeader = true;
        this.includeColumnHeader = true;
    }

    /**
     * Returns the title for row header column
     * @return  title for row header
     */
    String getTitle() {
        return title;
    }

    /**
     * Returns the text to write for null values
     * @return  the text for null values
     */
    String getNullText() {
        return nullText;
    }

    /**
     * Returns the formats to control output formats
     * @return  the formats
     */
    Formats getFormats() {
        return formats;
    }

    /**
     * Returns the separator for output
     * @return  the separator, by default a comma
     */
    String getSeparator() {
        return separator;
    }

    /**
     * Returns the output resource object
     * @return  the output resource to write to
     */
    Resource getResource() {
        return resource;
    }

    /**
     * Returns true if the row header should be included in output
     * @return      true if row header should be included
     */
    boolean isIncludeRowHeader() {
        return includeRowHeader;
    }

    /**
     * Returns true if the column header should be included in output
     * @return  true if column header should be included
     */
    boolean isIncludeColumnHeader() {
        return includeColumnHeader;
    }

    /**
     * Returns the row key printer for these options
     * @return      the row key printer
     */
    Optional<Printer<R>> getRowKeyPrinter() {
        return Optional.ofNullable(rowKeyPrinter);
    }

    /**
     * Sets the title for top left most cell
     * @param title     the top left cell title
     */
    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * Sets the null text to print for null values
     * @param nullText  the null text
     */
    public void setNullText(String nullText) {
        this.nullText = nullText;
    }

    /**
     * Sets the formats to use for output to CSV
     * @param formats   the formats to apply
     */
    public void setFormats(Formats formats) {
        this.formats = formats;
    }

    /**
     * Sets the formats to use for output to CSV
     * @param configure   the formats to apply
     */
    public void setFormats(Consumer<Formats> configure) {
        if (formats != null) {
            configure.accept(formats);
        } else {
            this.formats = new Formats();
            configure.accept(formats);
        }
    }

    /**
     * Sets the function to format row keys as strings
     * @param rowKeyPrinter     the row key printer
     */
    public void setRowKeyPrinter(Printer<R> rowKeyPrinter) {
        this.rowKeyPrinter = rowKeyPrinter;
    }

    /**
     * Sets the file path to write output to
     * @param path  the fully qualified file path
     */
    public void setFile(String path) {
        this.resource = Resource.of(new File(path));
    }

    /**
     * Sets the file handle to write output to
     * @param file  the file handle
     */
    public void setFile(File file) {
        this.resource = Resource.of(file);
    }

    /**
     * Sets the output stream to write output to
     * @param os    the output stream
     */
    public void setOutputStream(OutputStream os) {
        this.resource = Resource.of(os);
    }

    /**
     * Sets the separator to use, which by default is a comma
     * @param separator     the separator for output
     */
    public void setSeparator(String separator) {
        this.separator = separator;
    }

    /**
     * Sets the resource to write CSV output to
     * @param resource  the resource to write to
     */
    public void setResource(Resource resource) {
        this.resource = resource;
    }

    /**
     * Sets whether to include row keys in output
     * @param includeRowHeader  true to include row keys
     */
    public void setIncludeRowHeader(boolean includeRowHeader) {
        this.includeRowHeader = includeRowHeader;
    }

    /**
     * Sets whether to include column keys in output
     * @param includeColumnHeader   true to include column keys
     */
    public void setIncludeColumnHeader(boolean includeColumnHeader) {
        this.includeColumnHeader = includeColumnHeader;
    }
}
