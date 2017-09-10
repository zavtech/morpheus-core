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
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A utility class that provides some useful I/O related methods
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class IO {

    private static final IO instance = new IO();

    /**
     * Private constructor
     */
    private IO() {
        super();
    }

    /**
     * More concise way for write to standard out
     * @param format    the format string
     * @param args      the format args
     * @return          this IO
     */
    public static IO printf(String format, Object... args) {
        System.out.printf(format, args);
        return instance;
    }


    /**
     * More concise way for write to standard out
     * @param value     the value to print
     * @return          this IO
     */
    public static IO println(Object value) {
        System.out.println(value);
        return instance;
    }


    /**
     * Closes a closable while swallowing any potential exceptions
     * @param closeable     the closeable, can be null
     * @return              the IO instance
     */
    public static IO close(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return instance;
    }


    /**
     * Writes text out to the file creating any necessary parent directories
     * @param text      the text to write to file
     * @param file      the file to write to
     * @throws IOException  if there is an IO exception
     */
    public static void writeText(String text, File file) throws IOException {
        final File dir = file.getParentFile();
        if (dir != null && !dir.exists()) {
            if (!dir.mkdirs()) {
                throw new RuntimeException("Unable to create one or more directories at " + dir.getAbsolutePath());
            }
        }
        writeText(text, new FileOutputStream(file));
    }


    /**
     * Writes text to the output stream specified
     * @param text      the text to write
     * @param os        the output stream to write to
     * @throws IOException  if there is an I/) error
     */
    public static void writeText(String text, OutputStream os) throws IOException {
        if (os instanceof BufferedOutputStream) {
            try {
                os.write(text.getBytes("UTF-8"));
            } finally {
                close(os);
            }
        } else {
            writeText(text, new BufferedOutputStream(os));
        }
    }


    /**
     * Reads bytes from the input stream as text
     * @param is        the input stream to read from
     * @return              the resulting text
     * @throws IOException  if there is an I/O exception
     */
    public static String readText(InputStream is) throws IOException {
        return readText(is, 1024 * 100);
    }


    /**
     * Reads bytes from the input stream as text
     * @param is        the input stream to read from
     * @param bufferSize    the size of the byte buffer
     * @return              the resulting text
     * @throws IOException  if there is an I/O exception
     */
    public static String readText(InputStream is, int bufferSize) throws IOException {
        if (is instanceof BufferedInputStream) {
            try {
                final byte[] buffer = new byte[bufferSize];
                final StringBuilder result = new StringBuilder();
                while (true) {
                    final int read = is.read(buffer);
                    if (read < 0) break;
                    else {
                        result.append(new String(buffer, 0, read));
                    }
                }
                return result.toString();
            } finally {
                close(is);
            }
        } else {
            return readText(new BufferedInputStream(is), bufferSize);
        }
    }

}
