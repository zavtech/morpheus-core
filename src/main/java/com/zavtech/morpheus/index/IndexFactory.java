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
package com.zavtech.morpheus.index;

/**
 * A factory class that manufactures Index instances.
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
abstract class IndexFactory {

    private static IndexFactory instance;

    /**
     * Returns the singleton instance of this class
     * @return      the singleton index factory
     */
    public static synchronized IndexFactory getInstance() {
        if (instance == null) {
            instance = new IndexFactoryDefault();
        }
        return instance;
    }

    /**
     * Returns a newly created index from the iterable set of keys provided
     * @param keys      the array to create index from
     * @param <K>       the index element type
     * @return          the newly created Index.
     */
    public abstract <K> Index<K> create(Iterable<K> keys);

    /**
     * Returns a newly created index for the type and with initial size
     * @param keyType          the array type for index
     * @param initialSize   the initial size of index
     * @param <K>           the index element type
     * @return              the newly created Index.
     */
    public abstract <K> Index<K> create(Class<K> keyType, int initialSize);

}
