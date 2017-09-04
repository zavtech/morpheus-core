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

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
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


    public static IO printf(String format, Object... args) {
        System.out.printf(format, args);
        return instance;
    }

    public static IO println(Object value) {
        System.out.println(value);
        return instance;
    }


    public static void writeText(String text, File file) throws IOException {
        final File dir = file.getParentFile();
        if (dir != null && dir.mkdirs()) {
            System.out.println("Created directory: " + dir.getAbsolutePath());
        }
        writeText(text, new FileOutputStream(file));
    }


    public static void writeText(String text, OutputStream os) throws IOException {
        try (BufferedOutputStream bos = new BufferedOutputStream(os)) {
            bos.write(text.getBytes("UTF-8"));
        }
    }

}
