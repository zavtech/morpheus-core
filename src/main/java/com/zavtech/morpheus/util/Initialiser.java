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

import java.util.function.Consumer;

/**
 * A utility class that provides various patterns for initialising objects.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class Initialiser {

    /**
     * Applies the instance to the configurator and returns the result
     * @param instance      the instance to configure
     * @param configuarator the configurator to configure instance
     * @param <T>           the instance type
     * @return              the configured instance
     */
    public static <T> T apply(T instance, Consumer<T> configuarator) {
        configuarator.accept(instance);
        return instance;
    }

    /**
     * Returns a new instance of the type specified, which can be initialized with the consumer provided
     * @param type          the class of the type to create, which must have a no-argument constructor
     * @param configurator  the consumer that can be used to configure the newly created instance before it is returned
     * @param <T>           the type for object to manufacture and configure
     * @return              the newly created object after it has visited the consumer
     */
    public static <T> T apply(Class<T> type, Consumer<T> configurator) {
        try {
            final T instance = type.newInstance();
            configurator.accept(instance);
            return instance;
        } catch (Exception ex) {
            throw new RuntimeException("Failed to initialize instance of " + type, ex);
        }
    }
}
