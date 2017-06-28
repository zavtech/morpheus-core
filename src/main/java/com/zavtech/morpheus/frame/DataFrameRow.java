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

/**
 * A convenience marker interface used to represent a row vector on a DataFrame
 *
 * The <code>DataFrameVector</code> interface is parameterized in 5 types, which makes a it
 * rather cumbersome to pass around directly. The <code>DataFrameRow</code> and <code>DataFrameColumn</code>
 * interfaces exist to address this, and also provide a strongly typed interface to distinguish
 * row vectors from column vectors should that be desirable.
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameRow<R,C> extends DataFrameVector<R,C,R,C,DataFrameRow<R,C>> {

}
