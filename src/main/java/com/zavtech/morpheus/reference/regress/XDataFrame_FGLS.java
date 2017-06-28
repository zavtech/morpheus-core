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
package com.zavtech.morpheus.reference.regress;

import java.util.List;

import com.zavtech.morpheus.frame.DataFrame;

/**
 * An implementation of the DataFrameLeastSquares interface of a Feasible Generalized Least Squares model based on the Cochrane-Orcutt iterative procedure
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class XDataFrame_FGLS<R,C> extends XDataFrameLeastSquares<R,C> {

    private int maxIterations;

    /**
     * Constructor
     * @param frame         the frame to operate on
     * @param regressand    the column key of the regressand
     * @param regressors    the column keys of regressors
     * @param maxIterations the max number of iterations
     */
    XDataFrame_FGLS(DataFrame<R,C> frame, C regressand, List<C> regressors, int maxIterations) {
        super("FGLS", frame, regressand, regressors, false);
        this.maxIterations = maxIterations;
    }

    @Override
    public void compute() {

    }
}
