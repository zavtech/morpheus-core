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

import java.lang.reflect.Method;

/**
 * An interface to a component that can estimate the amount of memory an object consumes.
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface MemoryEstimator {

    /**
     * Returns an estimate of how much memory an object consumes.
     * @param instance  the object instance
     * @return          the estimated memory in bytes
     */
    long getObjectSize(Object instance);


    /**
     * A MemoryEstimator implementation that leverages org.github.jamm.MemoryMeter
     */
    class DefaultMemoryEstimator implements MemoryEstimator {

        private Object meter;
        private Method measureDeep;

        /**
         * Constructor
         */
        public DefaultMemoryEstimator() {
            try {
                final Class<?> clazz = Class.forName("org.github.jamm.MemoryMeter");
                final Method method = clazz.getDeclaredMethod("hasInstrumentation");
                final boolean instrumentation = (Boolean) method.invoke(clazz);
                if (instrumentation) {
                    this.meter = clazz.newInstance();
                    this.measureDeep = clazz.getDeclaredMethod("measureDeep", Object.class);
                }
            } catch (ClassNotFoundException ex) {
                System.err.println("Unable to initialize MemoryMeter, class not found");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        @Override
        public long getObjectSize(Object instance) {
            try {
                final Object result = measureDeep != null ? measureDeep.invoke(meter, instance) : -1;
                return result instanceof Number ? ((Number)result).longValue() : -1;
            } catch (Exception ex) {
                throw new RuntimeException("Failed to measure memory for instance: " + instance, ex);
            }
        }
    }

}
