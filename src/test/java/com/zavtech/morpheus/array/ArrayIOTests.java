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
package com.zavtech.morpheus.array;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests for Array IO
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class ArrayIOTests {

    private final File directory = new File("/tmp/morpheus/tests");

    @BeforeTest
    public void deleteFiles() {
        if (directory.exists() && directory.isDirectory()) {
            final File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isFile() && file.getName().endsWith(".ser")) {
                        if (file.delete()) {
                            System.out.println("Deleted file " + file.getAbsolutePath());
                        }
                    }
                }
            }
        }
    }


    @DataProvider(name = "types")
    public Object[][] types() {
        return new ArraysBasicTests().types();
    }


    @SuppressWarnings("unchecked")
    @Test(dataProvider = "types")
    public <T> void testIO1(Class<T> type, ArrayStyle style) throws Exception {
        ObjectOutputStream os = null;
        ObjectInputStream is = null;
        final Array<T> array1 = ArraySortTests.random(type, 5000, style);
        try {
            final File file = new File(directory, "Array-" + array1.typeCode().name() + "-" + array1.style().isSparse() + ".ser");
            os = createOutputStream(file);
            os.writeObject(array1);
            os.close();
            is = createInputStream(file);
            final Array<T> array2 = (Array<T>)is.readObject();
            Assert.assertEquals(array1, array2);
        } finally {
            if (os != null) os.close();
            if (is != null) is.close();
        }
    }

    @SuppressWarnings("unchecked")
    @Test(dataProvider = "types")
    public <T> void testIO2(Class<T> type, ArrayStyle style) throws Exception {
        ObjectOutputStream os = null;
        ObjectInputStream is = null;
        final Array<T> array1 = ArraySortTests.random(type, 5000, style);
        final int[] indexes = new int[] { 5, 10, 14, 100, 234, 456, 787, 999, 1340, 3450, 4566 };
        final Array<T> array2 = Array.of(type, indexes.length, ArrayType.defaultValue(type), style);
        try {
            final File file = new File(directory, "Array-" + array1.typeCode().name() + "-" + array1.style().isSparse() + ".ser");
            os = createOutputStream(file);
            array1.write(os, indexes);
            os.close();
            is = createInputStream(file);
            array2.read(is, indexes.length);
            for (int i=0; i<indexes.length; ++i) {
                final T actual = array2.getValue(i);
                final T expected = array1.getValue(indexes[i]);
                Assert.assertEquals(actual, expected, "Values match for index: " + i);
            }
        } finally {
            if (os != null) os.close();
            if (is != null) is.close();
        }
    }



    private ObjectInputStream createInputStream(File file) throws Exception {
        return new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)));
    }

    private ObjectOutputStream createOutputStream(File file) throws Exception {
        final File directory = file.getParentFile();
        final boolean success = directory.mkdirs();
        if (!success && !directory.exists()) System.err.println("Failed to create directory: " + directory.getAbsolutePath());
        return new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
    }

}
