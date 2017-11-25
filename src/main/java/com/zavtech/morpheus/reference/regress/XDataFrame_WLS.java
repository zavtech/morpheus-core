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
import java.util.stream.DoubleStream;

import org.apache.commons.math3.linear.DiagonalMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;

/**
 * A Weighted Least Squares implementation of the DataFrameLeastSquares interface.
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class XDataFrame_WLS<R,C> extends XDataFrameLeastSquares<R,C> {

    private Array<Double> weights;

    /**
     * Constructor
     * @param frame         the frame to operate on
     * @param regressand    the column key of the regressand
     * @param regressors    the column keys of regressors
     * @param intercept     true to include an intercept / constant term in the model
     * @param weights       the diagonal weighting matrix for GLS
     */
    XDataFrame_WLS(DataFrame<R,C> frame, C regressand, List<C> regressors, boolean intercept, Array<Double> weights) {
        super("WLS", frame, regressand, regressors, intercept);
        this.weights = weights;
    }


    @Override
    public void compute() {
        try {
            final RealVector y = createY();
            final RealMatrix x = createX();
            final DoubleStream weightSqrt = weights.stream().doubles().map(Math::sqrt);
            final RealMatrix p = new DiagonalMatrix(weightSqrt.toArray());
            final RealVector whiteY = p.operate(y);
            final RealMatrix whiteX = p.multiply(x);
            this.compute(whiteY, whiteX);
        } catch (DataFrameException ex) {
            throw ex;
        } catch (Exception ex) {
            final String regressors = Arrays.toString(getRegressors().toArray());
            final String message = "WLS regression failed for %s on %s";
            throw new DataFrameException(String.format(message, getRegressand(), regressors), ex);
        }
    }


    @Override
    protected double computeTSS(RealVector y) {
        if (!hasIntercept()) {
            return y.dotProduct(y);
        } else {
            final C regressand = getRegressand();
            final double sumOfWeights = weights.stats().sum().doubleValue();
            final Array<Double> yValues = Array.of(frame().col(regressand).toDoubleStream().toArray());
            final double weightedAvg = yValues.mapToDoubles(v -> v.getDouble() * weights.getDouble(v.index())).stats().sum().doubleValue() / sumOfWeights;
            final Array<Double> diffSquared = yValues.mapToDoubles(v -> weights.getDouble(v.index()) * Math.pow(v.getDouble() - weightedAvg, 2d));
            return diffSquared.stats().sum().doubleValue();
        }
    }
}
