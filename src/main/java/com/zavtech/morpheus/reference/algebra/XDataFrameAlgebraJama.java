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
package com.zavtech.morpheus.reference.algebra;

import java.util.Optional;
import java.util.function.Function;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.jama.CholeskyDecomposition;
import com.zavtech.morpheus.jama.EigenDecomposition;
import com.zavtech.morpheus.jama.LUDecomposition;
import com.zavtech.morpheus.jama.Matrix;
import com.zavtech.morpheus.jama.QRDecomposition;
import com.zavtech.morpheus.jama.SingularValueDecomposition;
import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.util.LazyValue;

/**
 * An implementation of the DataFrameAlgebra interface that uses JAMA
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @see <a href="http://math.nist.gov/javanumerics/jama/">JAMA</a>
 * @author  Xavier Witdouck
 */
class XDataFrameAlgebraJama<R,C> extends XDataFrameAlgebra<R,C> {

    /**
     * Constructor
     * @param frame     the frame reference
     */
    XDataFrameAlgebraJama(DataFrame<R,C> frame) {
        super(frame);
    }

    @Override
    public Decomposition decomp() {
        return new Decomp(frame());
    }


    @Override
    public DataFrame<Integer,Integer> inverse() throws DataFrameException {
        try {
            final Matrix matrix = toMatrix(frame());
            final Matrix inverse = matrix.inverse();
            return toDataFrame(inverse);
        } catch (Exception ex) {
            throw new DataFrameException("Failed to compute inverse of DataFrame", ex);
        }
    }

    @Override
    public DataFrame<Integer,Integer> solve(DataFrame<?,?> rhs) throws DataFrameException {
        try {
            final Matrix b = toMatrix(rhs);
            final Matrix a = toMatrix(frame());
            final Matrix x =  a.solve(b);
            return toDataFrame(x);
        } catch (Exception ex) {
            throw new DataFrameException("Failed to solve AX=B for frames", ex);
        }
    }

    /**
     * Returns a Morpheus DataFrame representation of a JAMA matrix
     * @param matrix    the JAMA matrix
     * @return          the Morpheus DataFrame
     */
    private DataFrame<Integer,Integer> toDataFrame(Matrix matrix) {
        final Range<Integer> rowKeys = Range.of(0, matrix.getRowDimension());
        final Range<Integer> colKeys = Range.of(0, matrix.getColumnDimension());
        return DataFrame.ofDoubles(rowKeys, colKeys, v -> {
            final int i = v.rowOrdinal();
            final int j = v.colOrdinal();
            return matrix.get(i, j);
        });
    }

    /**
     * Returns a JAMA matrix representation of the Morpheus DataFrame
     * @param frame     the DataFrame reference
     * @return          the JAMA matrix
     */
    private Matrix toMatrix(DataFrame<?,?> frame) {
        final Matrix matrix = new Matrix(frame.rowCount(), frame.colCount());
        frame.forEachValue(v -> {
            final int i = v.rowOrdinal();
            final int j = v.colOrdinal();
            final double value = v.getDouble();
            matrix.set(i, j, value);
        });
        return matrix;
    }


    /**
     * The Decomposition implementation for Apache Library
     */
    private class Decomp implements Decomposition {

        private DataFrame<?,?> frame;

        /**
         * Constructor
         * @param frame     the frame reference
         */
        Decomp(DataFrame<?,?> frame) {
            this.frame = frame;
        }

        @Override
        public <T> Optional<T> lud(Function<LUD, Optional<T>> handler) {
            return handler.apply(new XLUD(toMatrix(frame)));
        }

        @Override
        public <T> Optional<T> qrd(Function<QRD, Optional<T>> handler) {
            return handler.apply(new XQRD(toMatrix(frame)));
        }

        @Override
        public <T> Optional<T> evd(Function<EVD, Optional<T>> handler) {
            return handler.apply(new XEVD(toMatrix(frame)));
        }

        @Override
        public <T> Optional<T> svd(Function<SVD, Optional<T>> handler) {
            return handler.apply(new XSVD(toMatrix(frame)));
        }

        @Override
        public <T> Optional<T> cd(Function<CD, Optional<T>> handler) {
            return handler.apply(new XCD(toMatrix(frame)));
        }

    }


    /**
     * An implementation of LU Decomposition using the Apache Library
     */
    private class XLUD implements LUD {

        private LUDecomposition lud;
        private LazyValue<DataFrame<Integer,Integer>> l;
        private LazyValue<DataFrame<Integer,Integer>> u;
        private LazyValue<DataFrame<Integer,Integer>> p;

        /**
         * Constructor
         * @param matrix    the matrix to decompose
         */
        private XLUD(Matrix matrix) {
            this.lud = new LUDecomposition(matrix);
            this.l = LazyValue.of(() -> toDataFrame(lud.getL()));
            this.u = LazyValue.of(() -> toDataFrame(lud.getU()));
            this.p = LazyValue.of(() -> toDataFrame(null));
        }

        @Override
        public double det() {
            return lud.det();
        }

        @Override
        public boolean isNonSingular() {
            return lud.isNonsingular();
        }

        @Override
        public DataFrame<Integer,Integer> getL() {
            return l.get();
        }

        @Override
        public DataFrame<Integer,Integer> getU() {
            return u.get();
        }

        @Override
        public DataFrame<Integer,Integer> getP() {
            return p.get();
        }

        @Override
        public DataFrame<Integer,Integer> solve(DataFrame<?,?> rhs) {
            final Matrix b = toMatrix(rhs);
            final Matrix result = lud.solve(b);
            return toDataFrame(result);
        }
    }


    /**
     * An implementation of an QR Decomposition using the Apache Library
     */
    private class XQRD implements QRD {

        private QRDecomposition qrd;
        private LazyValue<DataFrame<Integer,Integer>> q;
        private LazyValue<DataFrame<Integer,Integer>> r;

        /**
         * Constructor
         * @param matrix    the input matrix
         */
        XQRD(Matrix matrix) {
            this.qrd = new QRDecomposition(matrix);
            this.q = LazyValue.of(() -> toDataFrame(qrd.getQ()));
            this.r = LazyValue.of(() -> toDataFrame(qrd.getR()));
        }

        @Override
        public DataFrame<Integer,Integer> getR() {
            return r.get();
        }

        @Override
        public DataFrame<Integer,Integer> getQ() {
            return q.get();
        }

        @Override
        public DataFrame<Integer,Integer> solve(DataFrame<?,?> rhs) {
            final Matrix b = toMatrix(rhs);
            final Matrix result = qrd.solve(b);
            return toDataFrame(result);
        }
    }


    /**
     * An implementation of an Eigenvalue Decomposition using the Apache Library
     */
    private class XEVD implements EVD {

        private Array<Double> eigenValues;
        private DataFrame<Integer,Integer> d;
        private DataFrame<Integer,Integer> v;

        /**
         * Constructor
         * @param matrix     the input matrix
         */
        XEVD(Matrix matrix) {
            final EigenDecomposition evd = new EigenDecomposition(matrix);
            this.d = toDataFrame(evd.getD());
            this.v = toDataFrame(evd.getV());
            this.eigenValues = Array.of(evd.getRealEigenvalues());
        }

        @Override
        public Array<Double> getEigenvalues() {
            return eigenValues;
        }

        @Override
        public DataFrame<Integer,Integer> getD() {
            return d;
        }

        @Override
        public DataFrame<Integer,Integer> getV() {
            return v;
        }
    }


    /**
     * An implementation of Singular Value Decomposition using the Apache Library
     */
    private class XSVD implements SVD {

        private int rank;
        private Array<Double> singularValues;
        private DataFrame<Integer,Integer> u;
        private DataFrame<Integer,Integer> v;
        private DataFrame<Integer,Integer> s;

        /**
         * Constructor
         * @param x     the input matrix
         */
        XSVD(Matrix x) {
            final SingularValueDecomposition svd = new SingularValueDecomposition(x);
            this.rank = svd.rank();
            this.u = toDataFrame(svd.getU());
            this.v = toDataFrame(svd.getV());
            this.s = toDataFrame(svd.getS());
            this.singularValues = Array.of(svd.getSingularValues());
        }

        @Override
        public final int rank() {
            return rank;
        }

        @Override
        public final DataFrame<Integer,Integer> getU() {
            return u;
        }

        @Override
        public final DataFrame<Integer,Integer> getV() {
            return v;
        }

        @Override
        public final DataFrame<Integer,Integer> getS() {
            return s;
        }

        @Override
        public final Array<Double> getSingularValues() {
            return singularValues;
        }
    }


    /**
     * An implementation of Cholesky Decomposition using the Apache Library
     */
    private class XCD implements CD {

        private CholeskyDecomposition cd;
        private LazyValue<DataFrame<Integer,Integer>> l;

        /**
         * Constructor
         * @param matrix    the input matrix
         */
        XCD(Matrix matrix) {
            this.cd = new CholeskyDecomposition(matrix);
            this.l = LazyValue.of(() -> toDataFrame(cd.getL()));
        }

        @Override
        public DataFrame<Integer,Integer> getL() {
            return l.get();
        }

        @Override
        public DataFrame<Integer,Integer> solve(DataFrame<?,?> rhs) {
            final Matrix b = toMatrix(rhs);
            final Matrix result = cd.solve(b);
            return toDataFrame(result);
        }
    }

}
