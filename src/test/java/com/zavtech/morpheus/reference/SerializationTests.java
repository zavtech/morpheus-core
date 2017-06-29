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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.range.Range;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests <code>DataFrame</code> serialization for various types by writing them to disk and then reading them back and comparing the results.
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class SerializationTests {

    private final File directory = new File("/tmp/morpheus/tests");

    /**
     * Constructor
     */
    public SerializationTests() {
        super();
    }

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


    @Test(description="Tests to ensure that a DataFrame can be serialized and de-serialized")
    @DataProvider(name="args")
    public Object[][] createArgs() throws Exception {
        final Index<Integer> keys = Range.of(0, 100).toIndex(Integer.class);
        return new Object[][]{
            {"DataFrameOfBooleans-dense.ser", TestDataFrames.random(boolean.class, keys, keys)},
            {"DataFrameOfIntegers-dense.ser", TestDataFrames.random(int.class, keys, keys)},
            {"DataFrameOfLongs-dense.ser", TestDataFrames.random(long.class, keys, keys)},
            {"DataFrameOfDoubles-dense.ser", TestDataFrames.random(double.class, keys, keys)},
            {"DataFrameOfObjects-dense.ser", TestDataFrames.random(String.class, keys, keys)},
            {"DataFrameSlice-dense.ser", TestDataFrames.random(double.class, keys, keys).select((k) -> true, (k) -> true)},
        };
    }


    @Test(dataProvider= "args")
    public <R,C> void testSerialization(String fileName, DataFrame<R,C> frame) throws Exception {
        final File file = new File(directory, fileName);
        try {
            this.writeObject(frame, file);
            final DataFrame result = (DataFrame)readObject(file);
            DataFrameAsserts.assertEqualsByIndex(result, frame);
        } finally {
            if (file.delete()) {
                System.out.println("Deleted file " + file.getAbsolutePath());
            }
        }
    }

    @Test(dataProvider= "args")
    public <R,C> void testSerializationOfTranspose(String fileName, DataFrame<R,C> frame) throws Exception {
        final File file = new File(directory, fileName);
        try {
            final DataFrame<C,R> transpose = frame.transpose();
            this.writeObject(transpose, file);
            final DataFrame result = (DataFrame)readObject(file);
            DataFrameAsserts.assertEqualsByIndex(result, transpose);
        } finally {
            if (file.delete()) {
                System.out.println("Deleted file " + file.getAbsolutePath());
            }
        }
    }


    /**
     * Reads an object from the file specified
     * @param file      the file reference
     * @return          the de-serialized object
     * @throws Exception    if operation fails
     */
    private Object readObject(File file) throws Exception {
        ObjectInputStream ois = null;
        try {
            System.out.println("Reading object from " + file.getAbsolutePath());
            ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)));
            return ois.readObject();
        } finally {
            try {
                if (ois != null) {
                    ois.close();
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    /**
     * Writes the object to the file specified
     * @param object    the object to write
     * @param file      the file reference
     * @throws Exception    if operation fails
     */
    private void writeObject(Object object, File file) throws Exception {
        ObjectOutputStream oos = null;
        try {
            final File directory = file.getParentFile();
            final boolean success = directory.mkdirs();
            if (!success && !directory.exists()) System.err.println("Failed to create directory: " + directory.getAbsolutePath());
            oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
            oos.writeObject(object);
            System.out.println("Wrote object to " + file.getAbsolutePath());
        } finally {
            try {
                if (oos != null) {
                    oos.close();
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

}
