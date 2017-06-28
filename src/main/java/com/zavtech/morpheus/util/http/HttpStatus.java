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
package com.zavtech.morpheus.util.http;

/**
 * A class that captures a status code and status message.
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class HttpStatus {

    private int code;
    private String message;

    /**
     * Constructor
     * @param code      the http status code
     * @param message   the http status message
     */
    HttpStatus(int code, String message) {
        this.code = code;
        this.message = message;
    }

    /**
     * Returns the http status code
     * @return  the http status code
     */
    public int getCode() {
        return code;
    }

    /**
     * Returns the http status message
     * @return  the http status message
     */
    public String getMessage() {
        return message;
    }
}
