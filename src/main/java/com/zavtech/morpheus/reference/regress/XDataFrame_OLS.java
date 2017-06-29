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

import java.util.Arrays;
import java.util.List;

import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;

/**
 * The reference implementation of the DataFrameOLS interface which exposes a Ordinary Least Squares Linear Regression model on column data.
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class XDataFrame_OLS<R,C> extends XDataFrameLeastSquares<R,C> {

    /**
     * Constructor
     * @param frame         the frame to operate on
     * @param regressand    the column key of the regressand
     * @param regressors    the column keys of regressors
     * @param intercept     true to include an intercept / constant term in the model
     */
    XDataFrame_OLS(DataFrame<R,C> frame, C regressand, List<C> regressors, boolean intercept) {
        super("OLS", frame, regressand, regressors, intercept);
    }


    @Override
    public void compute() {
        try {
            final RealVector y = createY();
            final RealMatrix x = createX();
            this.compute(y, x);
        } catch (DataFrameException ex) {
            throw ex;
        } catch (Exception ex) {
            final String regressors = Arrays.toString(getRegressors().toArray());
            final String message = "OLS regression failed for %s on %s";
            throw new DataFrameException(String.format(message, getRegressand(), regressors), ex);
        }
    }
}
