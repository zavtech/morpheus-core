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
package com.zavtech.morpheus.util.sql;

/**
 * Defines an enum of the SQL database platforms supported by Morpheus.
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public enum SQLPlatform {

    H2, HSQL, SQLITE, MYSQL, MSSQL, GENERIC;

    /**
     * Returns the database platfoem based on the JDBC driver class name
     * @param driverClassName   the fully qualified JDBC driver name
     * @return                  the sql platform for driver
     */
    public static SQLPlatform getPlatform(String driverClassName) {
        final String className = driverClassName.toLowerCase();
        if (className.contains("h2")) {
            return H2;
        } else if (className.contains("hsql")) {
            return HSQL;
        } else if (className.contains("sqlite")) {
            return SQLITE;
        } else if (className.contains("sqlserver")) {
            return MSSQL;
        } else if (className.contains("mysql")) {
            return MSSQL;
        } else {
            return GENERIC;
        }
    }

}
