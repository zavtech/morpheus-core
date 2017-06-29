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

import java.util.Optional;
import java.util.function.Function;

import com.zavtech.morpheus.array.Array;

/**
 * An interface that exposes various regression models that operate on column data in a Morpheus DataFrame
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameRegression<R,C> {

    /**
     * Executes a single variable linear regression model using Ordinary Least Squares (OLS)
     * @param regressand    the column key of the regressand or Y variable
     * @param regressor     the column key of the regressor or X variable
     * @param intercept     true to include an intercept / constant term in the model
     * @param handler       the regression result handler
     * @return              an optional result built from regression results
     */
    <T> Optional<T> ols(C regressand, C regressor, boolean intercept, Function<DataFrameLeastSquares<R,C>,Optional<T>> handler);

    /**
     * Executes a multiple variable linear regression model using Ordinary Least Squares (OLS)
     * @param regressand    the column that defines the regressand or Y variable
     * @param regressors    the column(s) that define the regressors or X variables
     * @param intercept     true to include an intercept / constant term in the model
     * @param handler       the regression result handler
     * @return              an optional result built from regression results
     */
    <T> Optional<T> ols(C regressand, Iterable<C> regressors, boolean intercept, Function<DataFrameLeastSquares<R,C>,Optional<T>> handler);

    /**
     * Executes a single variable linear regression model using Weighted Least Squares (WLS)
     * @param regressand    the column key of the regressand or Y variable
     * @param regressor     the column key of the regressor or X variable
     * @param weights       the weights for the diagonal matrix in WLS
     * @param intercept     true to include an intercept / constant term in the model
     * @param handler       the regression result handler
     * @return              an optional result built from regression results
     */
    <T> Optional<T> wls(C regressand, C regressor, Array<Double> weights, boolean intercept, Function<DataFrameLeastSquares<R,C>,Optional<T>> handler);

    /**
     * Executes a multiple variable linear regression model using Ordinary Least Squares (WLS)
     * @param regressand    the column that defines the regressand or Y variable
     * @param regressors    the column(s) that define the regressors or X variables
     * @param weights       the weights for the diagonal matrix in WLS
     * @param intercept     true to include an intercept / constant term in the model
     * @param handler       the regression result handler
     * @return              an optional result built from regression results
     */
    <T> Optional<T> wls(C regressand, Iterable<C> regressors, Array<Double> weights, boolean intercept, Function<DataFrameLeastSquares<R,C>,Optional<T>> handler);

    /**
     * Executes a single variable linear regression model using Generalized Least Squares (GLS)
     * @param regressand    the column that defines the regressand or Y variable
     * @param regressor     the column key of the regressor or X variable
     * @param omega         the covariance matrix of the error terms
     * @param intercept     true to include an intercept / constant term in the model
     * @param handler       the regression result handler
     * @param <T>           the type of return object
     * @return              the optional user created return object
     */
    <T> Optional<T> gls(C regressand, C regressor, DataFrame<?,?> omega, boolean intercept, Function<DataFrameLeastSquares<R,C>,Optional<T>> handler);

    /**
     * Executes a multiple variable linear regression model using Generalized Least Squares (GLS)
     * @param regressand    the column that defines the regressand or Y variable
     * @param regressors    the column(s) that define the regressors or X variables
     * @param omega         the covariance matrix of the error terms
     * @param intercept     true to include an intercept / constant term in the model
     * @param handler       the regression result handler
     * @param <T>           the type of return object
     * @return              the optional user created return object
     */
    <T> Optional<T> gls(C regressand, Iterable<C> regressors, DataFrame<?,?> omega, boolean intercept, Function<DataFrameLeastSquares<R,C>,Optional<T>> handler);

}
