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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * A simple wrapper class that can be used to carry a resource such as a File, URL, Input or Output stream.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class Resource {

    public enum Type { FILE, URL, INPUT_STREAM, OUTPUT_STREAM }

    private Type type;
    private Object target;

    /**
     * Constructor
     * @param target     the resource target
     */
    private Resource(Object target) {
        this.target = target;
        if (target == null) {
            throw new IllegalArgumentException("The resource target cannot be null");
        } else if (target instanceof File) {
            this.type = Type.FILE;
        } else if (target instanceof URL) {
            this.type = Type.URL;
        } else if (target instanceof InputStream) {
            this.type = Type.INPUT_STREAM;
        } else if (target instanceof OutputStream) {
            this.type = Type.OUTPUT_STREAM;
        } else {
            throw new IllegalArgumentException("Unsupported type specified: " + target);
        }
    }

    /**
     * Returns a resource wrapper around a file
     * @param file  the file reference
     * @return      the newly created resource wrapper
     */
    public static Resource of(File file) {
        return new Resource(file);
    }

    /**
     * Returns a resource wrapper around a url
     * @param url   the url reference
     * @return      the newly created resource wrapper
     */
    public static Resource of(URL url) {
        return new Resource(url);
    }

    /**
     * Returns a resource wrapper around a url
     * @param path the url string reference
     * @return          the newly created resource wrapper
     */
    public static Resource ofFile(String path) {
        return Resource.of(new File(path));
    }

    /**
     * Returns a resource wrapper around a url
     * @param urlString the url string reference
     * @return          the newly created resource wrapper
     */
    public static Resource ofUrl(String urlString) {
        try {
            return new Resource(new URL(urlString));
        } catch (MalformedURLException ex) {
            throw new IllegalArgumentException("Malformed URL string: " + urlString, ex);
        }
    }

    /**
     * Returns a resource wrapper based on the path specified
     * @param resource  the resource string which could be a URL, file path or classpath resource
     * @return          the newly created resource wrapper
     */
    public static Resource of(String resource) {
        try {
            if (resource.startsWith("http://") || resource.startsWith("https://")) {
                return Resource.of(new URL(resource));
            } else {
                final File file = new File(resource);
                if (file.exists()) {
                    return Resource.of(file);
                } else {
                    System.out.println("Looking for classpath resource: " + resource);
                    final URL url = Resource.class.getResource(resource);
                    if (url == null) {
                        throw new RuntimeException("Unable to locate a resource for " + resource);
                    } else {
                        return Resource.of(url);
                    }
                }
            }
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException("Failed to resolve resource for " + resource, ex);
        }
    }

    /**
     * Returns a resource wrapper around a input stream
     * @param is    the input stream reference
     * @return      the newly created resource wrapper
     */
    public static Resource of(InputStream is) {
        return new Resource(is);
    }

    /**
     * Returns a resource wrapper around a output stream
     * @param os    the output stream reference
     * @return      the newly created resource wrapper
     */
    public static Resource of(OutputStream os) {
        return new Resource(os);
    }

    /**
     * Returns the resource type
     * @return  the resource type
     */
    public Type getType() {
        return type;
    }

    /**
     * Returns the target for this resource
     * @param <T>   the type for target
     * @return      the target for this resource
     */
    @SuppressWarnings("unchecked")
    public <T> T getTarget() {
        return (T)target;
    }

    public boolean isFile() {
        return target instanceof File;
    }

    public boolean isUrl() {
        return target instanceof URL;
    }

    public boolean isInputStream() {
        return target instanceof InputStream;
    }

    public boolean isOutputStream() {
        return target instanceof OutputStream;
    }

    /**
     * Returns the resource as a file
     * @return  the file reference
     * @throws java.lang.ClassCastException if the resource is not of this type
     */
    public File asFile() {
        return (File) target;
    }

    /**
     * Returns the resource as a URL
     * @return  the url reference
     * @throws java.lang.ClassCastException if the resource is not of this type
     */
    public URL asURL() {
        return (URL) target;
    }

    /**
     * Returns the resource as a InputStream
     * @return  the input stream reference
     * @throws java.lang.ClassCastException if the resource is not of this type
     */
    public InputStream asInputStream() {
        return (InputStream) target;
    }

    public InputStream toInputStream() {
        try {
            if (isInputStream()) {
                return (InputStream)target;
            } else if (isFile()) {
                return new BufferedInputStream(new FileInputStream((File)target));
            } else if (isUrl()) {
                return new BufferedInputStream(((URL)target).openStream());
            } else {
                throw new RuntimeException("Cannot create InputStream from resource target:" + target);
            }
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException("Failed to create InputStream for resource: " + target, ex);
        }
    }

    /**
     * Returns the resource as a OutputStream
     * @return  the output stream reference
     * @throws java.lang.ClassCastException if the resource is not of this type
     */
    public OutputStream toOutputStream() {
        try {
            if (isOutputStream()) {
                final OutputStream os = getTarget();
                return os instanceof BufferedOutputStream ? os : new BufferedOutputStream(os);
            } else if (isFile()) {
                final File file = getTarget();
                final File dir = file.getParentFile();
                final boolean failed = dir != null && !dir.exists() && !dir.mkdirs();
                if (!failed) return new BufferedOutputStream(new FileOutputStream(file));
                throw new RuntimeException("Failed to create output directory for: " + file.getAbsolutePath());
            } else if (isUrl() && getTarget().toString().startsWith("http")) {
                final URL url = getTarget();
                throw new UnsupportedOperationException("Not supported for URL resource: " + this);
            } else {
                throw new UnsupportedOperationException("Not supported for resource: " + this);
            }
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException("Failed to transform resource into output stream: " + this, ex);
        }
    }

}
