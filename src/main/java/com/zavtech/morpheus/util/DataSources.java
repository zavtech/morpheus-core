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

import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;

/**
 * A class used to expose named DataSource instances to application code
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class DataSources {

    private static final DataSources defaultSources = new DataSources();

    private final Map<String,DataSource> sourceMap = new HashMap<>();


    /**
     * Returns the default DataSources for the application
     * @return  the default DataSources
     */
    public static DataSources getDefault() {
        return defaultSources;
    }


    /**
     * Registers a new DataSource against the name specified
     * @param name      the name for DataSource
     * @param source    the DataSource reference
     * @param override  if true, replace an existing source if one exits
     */
    public void register(String name, DataSource source, boolean override) {
        Asserts.notNull(name, "The name cannot be null");
        Asserts.notNull(source, "The data source cannot be null");
        if (sourceMap.containsKey(name) && !override) {
            throw new IllegalArgumentException("A DataSource named " + name + "already exists");
        } else {
            sourceMap.put(name, source);
        }
    }

    /**
     * Returns the DataSource for the name specified
     * @param name  the DataSource name
     * @return      the matching DataSource
     * @throws IllegalArgumentException if no source registered for name
     */
    public DataSource getDataSource(String name) {
        Asserts.notNull(name, "The DataSource name cannot be null");
        final DataSource source = sourceMap.get(name);
        if (source != null) {
            return source;
        } else {
            throw new IllegalArgumentException("No DataSource exists for " + name);
        }
    }

}
