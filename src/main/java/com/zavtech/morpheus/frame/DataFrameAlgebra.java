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
 * An interface that defines the supported algebraic operations on a Morpheus DataFrame.
 *
 * @param <R>   the frame row key type
 * @param <C>   the frame column key type
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameAlgebra<R,C> {

    enum Lib { JAMA, APACHE }

    ThreadLocal<Lib> LIBRARY = new ThreadLocal<Lib>() {
        @Override
        protected Lib initialValue() {
            return Lib.APACHE;
        }
    };

    /**
     * Returns a reference to the DataFrame decomposition engine
     * @return      the DataFrame decomposition engine
     */
    Decomposition decomp();

    /**
     * Returns the inverse of this frame
     * @return      the inverse of this frame
     * @throws DataFrameException  if non numeric, or data frame is singular
     */
    DataFrame<Integer,Integer> inverse() throws DataFrameException;

    /**
     * Returns the solution of A*X = B
     * @param rhs   the right hand side data frame, B
     * @return      the solution data frame X
     * @throws DataFrameException  if non numeric or data frame is singular
     */
    DataFrame<Integer,Integer> solve(DataFrame<?,?> rhs) throws DataFrameException;

    /**
     * Returns the result of scalar addition, C = A + scalar
     * @param scalar    the scalar to add
     * @return          the resulting frame, C = A + scalar
     * @throws DataFrameException  if this data frame is non numeric
     */
    DataFrame<R,C> plus(Number scalar) throws DataFrameException;

    /**
     * Returns the result of element by element addition, C = A + frame
     * @param other     the frame to add
     * @return          the resulting frame, C = A + frame
     * @throws DataFrameException  if non numeric or dimensions are incompatible
     */
    DataFrame<R,C> plus(DataFrame<?,?> other) throws DataFrameException;

    /**
     * Returns the result of scalar subtraction, C = A - scalar
     * @param scalar    the scalar to subtract
     * @return          the resulting frame, C = A - scalar
     * @throws DataFrameException  if this frame is non numeric
     */
    DataFrame<R,C> minus(Number scalar) throws DataFrameException;

    /**
     * Returns the result of element by element subtraction, C = A - frame
     * @param other     the frame to subtract
     * @return          the resulting frame, C = A - frame
     * @throws DataFrameException  if non numeric or dimensions are incompatible
     */
    DataFrame<R,C> minus(DataFrame<?,?> other) throws DataFrameException;

    /**
     * Returns the result of scalar multiplication, C = scalar * A
     * @param scalar    the scalar to multiply by
     * @return          the resulting frame, C = scalar * A
     * @throws DataFrameException  if this frame is non numeric
     */
    DataFrame<R,C> times(Number scalar) throws DataFrameException;

    /**
     * Returns the result of element by element multiplication, C = A .* frame
     * @param other     the frame to multiply by
     * @return          the resulting frame, C = A .* frame
     * @throws DataFrameException  if non numeric or dimensions are incompatible
     */
    DataFrame<R,C> times(DataFrame<?,?> other) throws DataFrameException;

    /**
     * Returns the result of a dot product, C = A * right
     * The inner dimensions must match in that the column count of this frame must
     * match the row count of the argument frame otherwise an exception will be thrown.
     * @param right the right frame operand for dot product
     * @return      the resulting frame, C = A * right
     * @throws DataFrameException  if non numeric or dimensions are incompatible
     */
    <X,Y> DataFrame<R,Y> dot(DataFrame<X,Y> right) throws DataFrameException;

    /**
     * Returns the result of scalar division, C = A / scalar
     * @param scalar    the scalar denominator
     * @return          the resulting frame, C = A / scalar
     * @throws DataFrameException  if this frame is non numeric
     */
    DataFrame<R,C> divide(Number scalar) throws DataFrameException;

    /**
     * Returns the result of element by element division, C = A / denominator
     * @param other the frame denominator
     * @return      the resulting frame, C = A / denominator
     * @throws DataFrameException  if non numeric or dimensions are incompatible
     */
    DataFrame<R,C> divide(DataFrame<?,?> other) throws DataFrameException;


    /**
     * An interface that exposes a convenient API to execute common Linear Algebra decompositions of a DataFrame
     */
    interface Decomposition {

        <T> Optional<T> lud(Function<LUD,Optional<T>> handler);

        <T> Optional<T> qrd(Function<QRD,Optional<T>> handler);

        <T> Optional<T> evd(Function<EVD,Optional<T>> handler);

        <T> Optional<T> svd(Function<SVD,Optional<T>> handler);

        <T> Optional<T> cd(Function<CD,Optional<T>> handler);

    }

    /**
     * Interface to LU Decomposition results
     * @see <a href="https://en.wikipedia.org/wiki/LU_decomposition">LU Decomposition</a>
     */
    interface LUD {

        double det();

        boolean isNonSingular();

        DataFrame<Integer,Integer> getL();

        DataFrame<Integer,Integer> getU();

        DataFrame<Integer,Integer> getP();

        DataFrame<Integer,Integer> solve(DataFrame<?,?> rhs);

    }

    /**
     * Interface to QR Decomposition results
     * @see <a href="https://en.wikipedia.org/wiki/QR_decomposition">QR Decomposition</a>
     */
    interface QRD {

        DataFrame<Integer,Integer> getR();

        DataFrame<Integer,Integer> getQ();

        DataFrame<Integer,Integer> solve(DataFrame<?,?> rhs);

    }

    /**
     * Interface to EigenValue Decomposition results
     * @see <a href="https://en.wikipedia.org/wiki/Eigendecomposition_of_a_matrix">Eigen Decomposition</a>
     */
    interface EVD {

        Array<Double> getEigenvalues();

        DataFrame<Integer,Integer> getD();

        DataFrame<Integer,Integer> getV();

    }

    /**
     * Interface to Singular Value Decomposition results
     * @see <a href="https://en.wikipedia.org/wiki/Singular_value_decomposition">Singular Value Decomposition</a>
     */
    interface SVD {

        int rank();

        DataFrame<Integer,Integer> getU();

        DataFrame<Integer,Integer> getV();

        DataFrame<Integer,Integer> getS();

        Array<Double> getSingularValues();

    }

    /**
     * Interface to Cholesky Decomposition results
     * @see <a href="https://en.wikipedia.org/wiki/Cholesky_decomposition">Cholesky Decomposition</a>
     */
    interface CD {

        DataFrame<Integer,Integer> getL();

        DataFrame<Integer,Integer> solve(DataFrame<?,?> rhs);

    }

}
