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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A request descriptor used to make http requests of various kinds via the Morpheus http client api.
 *
 * @param <T>   the type produced by this request
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class HttpRequest<T> implements java.io.Serializable, Cloneable {

    private static final long serialVersionUID = 1L;

    private URL url;
    private HttpMethod method;
    private int retryCount;
    private int readTimeout;
    private int connectTimeout;
    private Map<String,String> headers;
    private Map<String,String> cookies;
    private HttpClient.ResponseHandler<T> responseHandler;

    /**
     * Constructor
     * @param method    the HTTP request method
     */
    HttpRequest(HttpMethod method) {
        this.method = method;
        this.headers = new HashMap<>();
        this.cookies = new HashMap<>();
    }

    /**
     * Applies all state from the argument to this request
     * @param request   the request reference
     * @return          this request
     */
    HttpRequest<T> apply(HttpRequest<T> request) {
        this.url = request.url;
        this.method = request.method;
        this.retryCount = request.retryCount;
        this.readTimeout = request.readTimeout;
        this.connectTimeout = request.connectTimeout;
        this.headers.putAll(request.headers);
        this.cookies.putAll(request.cookies);
        this.responseHandler = request.responseHandler;
        return this;
    }

    /**
     * Sets the URL for this request
     * @param url   the URL for this request
     */
    public void setUrl(URL url) {
        this.url = url;
    }

    /**
     * Sets the URL for this request
     * @param url   the URL for this request
     */
    public void setUrl(String url) {
        try {
            this.url = new URL(url);
        } catch (MalformedURLException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    /**
     * Sets the retry count for this request
     * @param retryCount    the retry count
     */
    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    /**
     * Sets the read timeout for this request
     * @param readTimeout   the read timeout in millis
     */
    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    /**
     * Sets the connect timeout for this request
     * @param connectTimeout    the connect timeout in millis
     */
    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    /**
     * Sets the value of the Accept Http header
     * @param accept    the Accept value
     */
    public void setAccept(String accept) {
        if (accept == null) {
            this.headers.remove("Accept");
        } else {
            this.headers.put("Accept", accept);
        }
    }

    /**
     * Sets the value of the Content-Type http header
     * @param contentType   the content type
     */
    public void setContentType(String contentType) {
        if (contentType == null) {
            this.headers.remove("Content-Type");
        } else {
            this.headers.put("Content-Type", contentType);
        }
    }

    /**
     * Sets the value of the Content-Length http header value
     * @param contentLength the content length
     */
    public void setContentLength(Integer contentLength) {
        if (contentLength == null) {
            this.headers.remove("Content-Length");
        } else {
            this.headers.put("Content-Length", String.valueOf(contentLength));
        }
    }

    /**
     * Sets the response handler for this request
     * @param responseHandler   the response handler
     */
    public void setResponseHandler(HttpClient.ResponseHandler<T> responseHandler) {
        this.responseHandler = responseHandler;
    }

    /**
     * Returns the url for this request
     * @return  the url for this request
     */
    public URL getUrl() {
        return url;
    }

    /**
     * Returns he HTTP method for this request
     * @return  the http method
     */
    public HttpMethod getMethod() {
        return method;
    }

    /**
     * Returns the retry count for this request
     * @return  the retry count
     */
    public int getRetryCount() {
        return retryCount;
    }

    /**
     * Returns the read time out for this request
     * @return  the read timeout
     */
    public int getReadTimeout() {
        return readTimeout;
    }

    /**
     * Returns the connect timeout for this request
     * @return  the connect timeout
     */
    public int getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Returns the response handler for this request
     * @return      the response handler for this request
     */
    public Optional<HttpClient.ResponseHandler<T>> getResponseHandler() {
        return Optional.ofNullable(responseHandler);
    }

    /**
     * Returns the request header parameters for this request
     * @return  the map of headers
     */
    public Map<String,String> getHeaders() {
        return headers;
    }

    /**
     * Returns an map of cookies for this request
     * @return  the map of cookies
     */
    public Map<String,String> getCookies() {
        return cookies;
    }

    /**
     * Returns the optional content for this request
     * @return      the optional content for request
     */
    public Optional<byte[]> getContent() {
        return Optional.empty();
    }

    /**
     * Creates a copy of this request replacing the URL
     * @param url   the URL to replace
     * @return      the copy of this request
     */
    @SuppressWarnings("unchecked")
    public HttpRequest<T> copy(String url) {
        try {
            final HttpRequest<T> clone = (HttpRequest<T>)super.clone();
            clone.url = new URL(url);
            return clone;
        } catch (Exception ex) {
            throw new RuntimeException("Failed to clone HttpRequest", ex);
        }
    }

    @Override
    public String toString() {
        return "HttpRequest{" +
                "url=" + url +
                ", method='" + method + '\'' +
                ", retryCount=" + retryCount +
                ", readTimeout=" + readTimeout +
                ", connectTimeout=" + connectTimeout +
                ", headers=" + headers +
                '}';
    }
}
