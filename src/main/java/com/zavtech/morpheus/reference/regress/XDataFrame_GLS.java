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

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.jama.CholeskyDecomposition;
import com.zavtech.morpheus.jama.Matrix;

/**
 * The reference implementation of the DataFrameLeastSquares interface which exposes a Generalized Least Squares Linear Regression model on column data.
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class XDataFrame_GLS<R,C> extends XDataFrameLeastSquares<R,C> {

    private DataFrame<?,?> omega;

    /**
     * Constructor
     * @param frame         the frame to operate on
     * @param regressand    the column key of the regressand
     * @param regressors    the column keys of regressors
     * @param intercept     true to include an intercept / constant term in the model
     * @param omega         the omega matrix, covariance of the residuals
     */
    XDataFrame_GLS(DataFrame<R,C> frame, C regressand, List<C> regressors, boolean intercept, DataFrame<?,?> omega) {
        super("GLS", frame, regressand, regressors, intercept);
        this.omega = omega;
    }


    @Override
    public void compute() {
        try {
            final RealVector y = createY();
            final RealMatrix x = createX();
            final RealMatrix p = initTransformMatrixJanma(omega);
            final RealVector whiteY = p.operate(y);
            final RealMatrix whiteX = p.multiply(x);
            this.compute(whiteY, whiteX);
        } catch (DataFrameException ex) {
            throw ex;
        } catch (Exception ex) {
            final String regressors = Arrays.toString(getRegressors().toArray());
            final String message = "GLS regression failed for %s on %s";
            throw new DataFrameException(String.format(message, getRegressand(), regressors), ex);
        }
    }

    private RealMatrix initTransformMatrixJanma(DataFrame<?,?> omega) {
        Matrix matrix1 = new Matrix(omega.rowCount(), omega.colCount());
        omega.forEachValue(v -> {
            final int rowOrdinal = v.rowOrdinal();
            final int colOrdinal = v.colOrdinal();
            final double value = v.getDouble();
            matrix1.set(rowOrdinal, colOrdinal, value);
        });
        final Matrix inverse = matrix1.inverse();
        final CholeskyDecomposition cholesky = new CholeskyDecomposition(inverse);
        final Matrix result = cholesky.getL().transpose();
        final RealMatrix matrix = new Array2DRowRealMatrix(result.getRowDimension(), result.getColumnDimension());
        for (int i=0; i<result.getRowDimension(); ++i) {
            for (int j=0; j<result.getColumnDimension(); ++j) {
                final double value = result.get(i, j);
                matrix.setEntry(i, j, value);
            }
        }
        return matrix;
    }
}
