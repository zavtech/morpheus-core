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
package com.zavtech.morpheus.util;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * A convenience class to process text based files one line at time.
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public class TextStreamReader implements Closeable {

    private String line = null;
    private BufferedReader reader = null;

    /**
     * Constructor
     * @param is    the input stream
     */
    public TextStreamReader(InputStream is) {
        try {
            this.reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(is)));
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    /**
     * Returns true if there is another line to read
     * @return true if another line exists
     */
    public boolean hasNext() {
        try {
            this.line = reader.readLine();
            if (line != null) {
                return true;
            } else {
                this.close();
                return false;
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    /**
     * Returns the next line in this reader
     * @return the next line in reader
     */
    public String nextLine() {
        return line;
    }

    @Override()
    public void close() {
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            reader = null;
        }
    }
}
