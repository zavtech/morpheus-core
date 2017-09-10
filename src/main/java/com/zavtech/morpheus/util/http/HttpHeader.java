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
 * A class that captures a http header key value pair
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class HttpHeader implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private String key;
    private String value;

    /**
     * Constructor
     * @param key       the header key
     * @param value     the header value
     */
    public HttpHeader(String key, String value) {
        this.key = key;
        this.value = value;
    }

    /**
     * Returns the header key
     * @return  the header key
     */
    public String getKey() {
        return key;
    }

    /**
     * Returns the header value
     * @return  the header value
     */
    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HttpHeader)) return false;
        final HttpHeader that = (HttpHeader) o;
        if (!getKey().equals(that.getKey())) return false;
        return getValue().equals(that.getValue());

    }

    @Override
    public int hashCode() {
        int result = getKey().hashCode();
        result = 31 * result + getValue().hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "HttpHeader{" + "key='" + key + '\'' + ", value='" + value + '\'' + '}';
    }
}
