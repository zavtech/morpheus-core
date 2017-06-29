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

import java.util.List;

/**
 * An interface to a Linear Regression model based on column data in a DataFame.
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameLeastSquares<R,C> {

    enum Solver { INV, QR }
    enum Field { PARAMETER, STD_ERROR, T_STAT, P_VALUE, CI_LOWER, CI_UPPER }

    /**
     * Triggers this model to (re)compute parameters
     * @throws DataFrameException   if there is an error running regression
     */
    void fit();

    /**
     * Returns the sample size for this model
     * @return  the sample size for regression model
     */
    int getN();

    /**
     * Returns the regressand for this model
     * @return  the regressand for this model
     */
    C getRegressand();

    /**
     * Returns the list of regressors for this model
     * @return  the list of regressor for model
     */
    List<C> getRegressors();

    /**
     * Returns true if this model includes and intercept term
     * @return      true if this model includes an intercept term
     */
    boolean hasIntercept();

    /**
     * Returns the significance level to compute coefficient confidence intervals
     * @return      the significance level used to compute confidence intrevals for regression coefficients
     */
    double getAlpha();

    /**
     * Returns the R-squared statistic for this regression model
     * The R-squared statistic is defined by R<sup>2</sup> = 1 - RSS / TSS where
     * RSS is the Residual Sum of Squares and TSS is the Total Sum of Squares.
     * @return  the R-squared statistic for model
     */
    double getRSquared();

    /**
     * Returns the adjusted R-squared statistic for this regression model
     * The adjusted R-squared statistic is defined by  R<sup>2</sup><sub>adj</sub> = 1 - [RSS (n - 1)] / [TSS (n - p)]
     * where RSS is the Residual Sum of Squares, TSS is the Total Sum of Squares, n is the number of observations
     * and p is the number of parameters estimated, including the intercept term.
     * @return  the adjusted R-squared for model
     */
    double getRSquaredAdj();

    /**
     * Returns the standard error for this regression model
     * The standard error is defined as Std Error = Sqrt(RSS / N) where RSS is the
     * Residual Sum of Squares and N represents the degrees of freedom of the model.
     * @return  the standard error for model
     */
    double getStdError();

    /**
     * Returns the F-Statistic for this regression model
     * @see <a href="https://en.wikipedia.org/wiki/F-test">Wikipedia</a>
     * @return  the F-Statistic for model
     */
    double getFValue();

    /**
     * Returns the F-Statistic probability
     * @see <a href="https://en.wikipedia.org/wiki/F-test">Wikipedia</a>
     * @return  the F-Statistic probability for model
     */
    double getFValueProbability();

    /**
     * Returns the sum of squared deviations of Y from its mean
     * @see <a href="https://en.wikipedia.org/wiki/Total_sum_of_squares">Wikipedia</a>
     * @return  the sum of the squares of Y
     */
    double getTotalSumOfSquares();

    /**
     * Returns the explained sum of squares for the model
     * @see <a href="https://en.wikipedia.org/wiki/Explained_sum_of_squares">Wikipedia</a>
     * @return      the explain sum of squares
     */
    double getExplainedSumOfSquares();

    /**
     * Returns the sum of squared residuals.
     * @see <a href="https://en.wikipedia.org/wiki/Residual_sum_of_squares">Wikipedia</a>
     * @return residual sum of squares
     */
    double getResidualSumOfSquares();

    /**
     * Returns the Durbin-Watson score for the residuals to check of serial correlation
     * @see <a href="https://en.wikipedia.org/wiki/Durbin%E2%80%93Watson_statistic">Wikipedia</a>
     * @return  the Durbin-Watson score of residuals
     */
    double getDurbinWatsonStatistic();

    /**
     * Returns the intercept statistic for the field specified
     * @return  the intercept statistic for field
     */
    double getInterceptValue(Field field);

    /**
     * Returns the beta / slope statistic for the field specified
     * @param regressor the regressor key
     * @param field     the field statistic
     * @return          the beta statistic
     */
    double getBetaValue(C regressor, Field field);

    /**
     * Returns the DataFrame of beta / slope statistics, including parameter value, std error, t-stat, p-value
     * @return the [Kx5] DataFrame of beta statistics
     */
    DataFrame<C,Field> getBetas();

    /**
     * Returns the DataFrame of intercept statistics, including parameter value, std error, t-stat, p-value
     * @return the [1x5] DataFrame of intercept statistics
     */
    DataFrame<String,Field> getIntercept();

    /**
     * Returns a DataFrame of the the residuals for the model, ie u = y - X*b.
     * @return The [nx1] frame representing the residuals
     */
    DataFrame<R,String> getResiduals();

    /**
     * Returns a DataFrame of the fitted values for the model, namely y hat
     * @return The [nx1] frame representing the residuals
     */
    DataFrame<R,String> getFittedValues();

    /**
     * Returns an lx1 DataFrame representing the autocorrelation function of the residuals
     * @param maxLag    the max lag for ACF, which will also be the resulting frame row count
     * @return  an lx1 DataFrame of the residual Autocorrelation Function
     */
    DataFrame<Integer,String> getResidualsAcf(int maxLag);

    /**
     * Sets the Least Squares solver to use to calculate beta estimate
     * @param solver    the solver, either INV direct solution or QR for QR Decomposition of X
     * @return          the updated regression model
     */
    DataFrameLeastSquares<R,C> withSolver(Solver solver);

    /**
     * Sets the significance level used to compute the coefficient confidence intervals
     * @param alpha     the significance level, by default 0.05
     * @return          the updated regression model
     */
    DataFrameLeastSquares<R,C> withAlpha(double alpha);

    /**
     * Sets whether this model should include and intercept term
     * @param intercept     true to include an intercept term
     * @return          the updated regression model
     */
    DataFrameLeastSquares<R,C> withIntercept(boolean intercept);


}
