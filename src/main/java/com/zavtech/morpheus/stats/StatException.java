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
package com.zavtech.morpheus.stats;

/**
 * A RuntimeException that can be raised by the statistics framework
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class StatException extends RuntimeException {

    /**
     * Constructor
     *
     * @param msg the exception message
     */
    public StatException(String msg) {
        this(msg, null);
    }

    /**
     * Constructor
     *
     * @param msg   the exception message
     * @param cause the exception cause
     */
    public StatException(String msg, Throwable cause) {
        super(msg, cause);
    }

}
