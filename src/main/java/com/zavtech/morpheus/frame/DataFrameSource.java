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

import com.zavtech.morpheus.util.AssertException;

/**
 * An interface that returns a DataFrame from some underlying source, such as a database, web service and so on
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p>This is open source software released under the <a href="http://www.ap`ache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameSource<R,C,O extends DataFrameSource.Options<R,C>> {

    /**
     * Returns true if this source supports the options type
     * @param options   the options instance
     * @param <T>       the options type
     * @return          true if this source supports the options
     */
    <T extends Options<?,?>> boolean isSupported(T options);

    /**
     * Returns a <code>DataFrame</code> read from some underlying device
     * @param options       the options to load the frame with
     * @return              the <code>DataFrame</code> response
     * @throws DataFrameException  if this operation fails
     */
    DataFrame<R,C> read(O options) throws DataFrameException;


    /**
     * A marker interface for a source options descriptor
     * @param <X>   the row key type
     * @param <Y>   the column key type
     */
    interface Options<X,Y> {

        /**
         * Validates that all required options are set
         * @throws IllegalStateException    if not all required options are set
         */
        void validate();

        /**
         * Convenience static function to validate options and return the same
         * @param options   the options to validate
         * @param <R>       the row key type
         * @param <C>       the column key type
         * @param <O>       the options type
         * @return          the same as the argument
         * @throws IllegalStateException    if validation fails
         */
        static <R,C,O extends Options<R,C>> O validate(O options) throws IllegalStateException {
            try {
                options.validate();
                return options;
            } catch (AssertException ex) {
                throw new IllegalStateException(ex.getMessage(), ex);
            }
        }
    }

}
