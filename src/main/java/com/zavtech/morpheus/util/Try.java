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

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.function.Function;

/**
 * An extension of a Function that implements a try catch and throws a RuntimeException
 * @param <T>   the argument type
 * @param <R>   the return type
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public class Try<T,R> implements Function<T,R> {

    private TryCatch<T,R> function;


    /**
     * A functional interface to a function that throws an exception
     */
    @FunctionalInterface
    public interface TryCatch<T,R> {

        /**
         * Applies this function to the arg specified
         * @param arg   the argument to function
         * @return      the function result
         * @throws Exception    if the operation fails
         */
        R apply(T arg) throws Exception;
    }

    /**
     * Constructor
     * @param function  the function to wrap with a try catch
     */
    public Try(TryCatch<T,R> function) {
        this.function = function;
    }

    /**
     * Returns a new Try functiom wrapper around the user provided function
     * @param function  the user function
     * @param <T>       the input type
     * @param <R>       the output type
     * @return          the newly created function wrapper
     */
    public static <T,R> Try<T,R> with(TryCatch<T,R> function) {
        return new Try<>(function);
    }

    /**
     * Calls the run() method wrapped in a try catch and throws a RuntimeException if fails
     * @param runnable  the runnable to run
     */
    public static void run(Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    /**
     * Calls the get() method on the future, catching any Exception and re-throwing as a RuntimneException
     * @param future    the future to get result from
     */
    public static <T> T get(Future<T> future) {
        try {
            return future.get();
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }


    /**
     * Calls the call() method wrapped in a try catch and throws a RuntimeException if fails
     * @param callable  the callable to call
     * @param <R>       the type of return
     * @return          the returned value
     */
    public static <R> R call(Callable<R> callable) {
        try {
            return callable.call();
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }


    @Override
    public R apply(T arg) {
        try {
            return function.apply(arg);
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

}
