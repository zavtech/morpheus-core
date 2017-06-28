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

import java.lang.instrument.Instrumentation;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * A class that provides access to the Instrumentation interface for the running VM
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public class Agent {

    private static Instrumentation instrumentation;
    private static Set<Class<?>> stopClassSet = new HashSet<>();

    /**
     * Static initializer
     */
    static {
        stopClassSet.add(Boolean.class);
        stopClassSet.add(Short.class);
        stopClassSet.add(Integer.class);
        stopClassSet.add(Float.class);
        stopClassSet.add(Double.class);
        stopClassSet.add(String.class);
        stopClassSet.add(Date.class);
    }

    /**
     * Called as soon as the JVM has launched to apply Instrumentation class
     * @param agentArgs         the agent args
     * @param instrumentation   the instrumentation instance
     */
    public static void premain(String agentArgs, Instrumentation instrumentation) {
        Agent.instrumentation = instrumentation;
    }

    /**
     * Returns true if this agent has been initalized
     * @return  true if agent has been initialized
     */
    public static boolean isInitialized() {
        return instrumentation != null;
    }

    /**
     * Returns a reference to the instrumentation instance for this VM
     * @return  the instrumentation instance for VM
     */
    public static Instrumentation getInstrumentation() {
        return instrumentation;
    }

}
