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
package com.zavtech.morpheus.array.coding;

/**
 * A base interface that should be extended by all coding implementations
 *
 * @param <T>   the coding data type
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public interface Coding<T> extends java.io.Serializable {

    /**
     * Returns the data type for this coding
     * @return      the data type for this coding
     */
    Class<T> getType();


    /**
     * A convenience base class for building coding implementations
     * @param <T>   the coding type
     */
    abstract class BaseCoding<T> implements Coding<T> {

        private Class<T> type;

        /**
         * Constructor
         * @param type  the type for this coding
         */
        BaseCoding(Class<T> type) {
            this.type = type;
        }

        @Override
        public Class<T> getType() {
            return type;
        }
    }

}
