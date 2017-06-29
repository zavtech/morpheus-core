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
import java.util.function.Consumer;

import com.zavtech.morpheus.util.Resource;
import com.zavtech.morpheus.util.text.Formats;

/**
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class JsonSinkOptions {

    private Formats formats;
    private Resource resource;
    private String encoding;

    /**
     * Constructor
     */
    public JsonSinkOptions() {
        this.formats = new Formats();
        this.encoding = "UTF-8";
    }

    /**
     * Returns the formats to apply for these options
     * @return      the formats to apply
     */
    public Formats getFormats() {
        return formats;
    }

    /**
     * Returns the resource for these options
     * @return  the resource
     */
    public Resource getResource() {
        return resource;
    }

    /**
     * Returns the encoding for these options
     * @return      the encoding
     */
    public String getEncoding() {
        return encoding;
    }

    /**
     * Sets the resource for these options
     * @param resource  the resource to apply
     */
    public void setResource(Resource resource) {
        this.resource = resource;
    }

    /**
     * Sets the resource output stream for these options
     * @param os    the output stream to write to
     */
    public void setOutputStream(OutputStream os) {
        this.resource = Resource.of(os);
    }

    /**
     * Sets the resource file for these options
     * @param file  the output file to write to
     */
    public void setFile(File file) {
        this.resource = Resource.of(file);
    }

    /**
     * Sets the resource file for these options
     * @param path  the output file path to write to
     */
    public void setFile(String path) {
        this.resource = Resource.of(new File(path));
    }

    /**
     * Sets the string encoding to use on these options
     * @param encoding  the string encoding to use, for example UTF-8
     */
    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    /**
     * Sets the formats for these options
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
}
