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
package com.zavtech.morpheus;

import java.io.File;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.util.Tuple;

/**
 * One line summary here...
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class TestSuite {


    /**
     * Constructor
     */
    public TestSuite() {
        super();
    }

    /**
     * Returns the directory for test output
     * @return  directory for test output
     */
    public static File getOutputDir(String testName) {
        final String tmpDir = System.getProperty("java.io.tmpdir");
        return new File(tmpDir,  "morpheus-tests/" + testName);
    }


    /**
     * Returns the directory for test output
     * @return  directory for test output
     */
    public static File getOutputFile(String testName, String fileName) {
        return new File(getOutputDir(testName), fileName);
    }


    /**
     * Returns the UK ONS population dataset
     * @return  the UK ONS population dataset
     */
    public static DataFrame<Tuple,String> getPopulationDataset() {
        return DataFrame.read().csv(options -> {
            options.setResource("http://tinyurl.com/ons-population-year");
            options.setRowKeyParser(Tuple.class, row -> Tuple.of(Integer.parseInt(row[1]), row[2]));
            options.setExcludeColumns("Code");
            options.getFormats().setNullValues("-");
            options.setFormats(formats -> {
                formats.setNullValues("-");
                formats.copyParser(Double.class, "All Males");
                formats.copyParser(Double.class, "All Females");
                formats.copyParser(Double.class, "All Persons");
                formats.copyParser(Double.class, "[MF]\\s+\\d+");
            });
        });
    }

}
