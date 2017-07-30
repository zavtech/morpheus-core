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
package com.zavtech.morpheus.frame;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * A base class for building DataFrameSource implementations that define a unified interface for loading DataFrames from various data sources.
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p>This is open source software released under the <a href="http://www.ap`ache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public abstract class DataFrameSource<R,C,O extends DataFrameSource.Options<R,C>> {


    private static final Map<Object,DataFrameSource<?,?,?>> sourceMap = new HashMap<>();


    /**
     * Registers a DataFrameSource
     * @param source    the source to register
     */
    public static void register(DataFrameSource<?,?,?> source) {
        sourceMap.put(source.getClass(), source);
    }


    /**
     * Returns a DataFrameSource for the type specified
     * @param type      the source type
     * @param <R>       the row key type
     * @param <C>       the column key type
     * @param <O>       the source options type
     * @param <S>       the source type
     * @return          the matching source
     */
    @SuppressWarnings("unchecked")
    public static <R,C,O extends Options<R,C>,S extends DataFrameSource<R,C,O>> S lookup(Class<S> type) {
        final S source = (S)sourceMap.get(type);
        if (source == null) {
            throw new IllegalArgumentException("No DataFrameSource registered for " + type);
        } else {
            return source;
        }
    }


    /**
     * Returns a <code>DataFrame</code> read from some underlying device based on options configured by the arg
     * @param configurator  the options consumer to configure load options
     * @return              the <code>DataFrame</code> response
     * @throws DataFrameException  if this operation fails
     */
    public abstract DataFrame<R,C> read(Consumer<O> configurator) throws DataFrameException;


    /**
     * Applies the options to the configurator and then validates
     * @param options       the empty options instance
     * @param configurator  the configurator to configure instance
     * @param <X>           the options type
     * @return              the configured options
     */
    protected <X extends Options<R,C>> X initOptions(X options, Consumer<X> configurator) {
        configurator.accept(options);
        options.validate();
        return options;
    }


    /**
     * A marker interface for a source options descriptor
     * @param <X>   the row key type
     * @param <Y>   the column key type
     */
    public interface Options<X,Y> {

        /**
         * Validates that all required options are set
         * @throws IllegalStateException    if not all required options are set
         */
        void validate();

    }

}
