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

import java.util.Optional;
import java.util.function.Function;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameLeastSquares;
import com.zavtech.morpheus.frame.DataFrameRegression;
import com.zavtech.morpheus.util.Collect;

/**
 * The reference implementation of the DataFrameRegression interface
 *
 * @param <R>       the row key type
 * @param <C>       the column key type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class XDataFrameRegression<R,C> implements DataFrameRegression<R,C> {

    private DataFrame<R,C> frame;

    /**
     * Constructor
     * @param frame the frame to operate on
     */
    public XDataFrameRegression(DataFrame<R,C> frame) {
        this.frame = frame;
    }

    @Override
    public <T> Optional<T> ols(C regressand, C regressor, boolean intercept, Function<DataFrameLeastSquares<R,C>,Optional<T>> handler) {
        return handler.apply(new XDataFrame_OLS<>(frame, regressand, Collect.asList(regressor), intercept));
    }

    @Override
    public <T> Optional<T> ols(C regressand, Iterable<C> regressors, boolean intercept, Function<DataFrameLeastSquares<R, C>, Optional<T>> handler) {
        return handler.apply(new XDataFrame_OLS<>(frame, regressand, Collect.asList(regressors), intercept));
    }

    @Override
    public <T> Optional<T> wls(C regressand, C regressor, Array<Double> weights, boolean intercept, Function<DataFrameLeastSquares<R, C>, Optional<T>> handler) {
        return handler.apply(new XDataFrame_WLS<>(frame, regressand, Collect.asList(regressor), intercept, weights));
    }

    @Override
    public <T> Optional<T> wls(C regressand, Iterable<C> regressors, Array<Double> weights, boolean intercept, Function<DataFrameLeastSquares<R, C>, Optional<T>> handler) {
        return handler.apply(new XDataFrame_WLS<>(frame, regressand, Collect.asList(regressors), intercept, weights));
    }

    @Override
    public <T> Optional<T> gls(C regressand, C regressor, DataFrame<?, ?> omega, boolean intercept, Function<DataFrameLeastSquares<R, C>, Optional<T>> handler) {
        return handler.apply(new XDataFrame_GLS<>(frame, regressand, Collect.asList(regressor), intercept, omega));
    }

    @Override
    public <T> Optional<T> gls(C regressand, Iterable<C> regressors, DataFrame<?, ?> omega, boolean intercept, Function<DataFrameLeastSquares<R, C>, Optional<T>> handler) {
        return handler.apply(new XDataFrame_GLS<>(frame, regressand, Collect.asList(regressors), intercept, omega));
    }
}
