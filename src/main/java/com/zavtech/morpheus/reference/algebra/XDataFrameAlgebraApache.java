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
import java.util.stream.IntStream;

import org.apache.commons.math3.exception.NotStrictlyPositiveException;
import org.apache.commons.math3.exception.OutOfRangeException;
import org.apache.commons.math3.linear.AbstractRealMatrix;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.CholeskyDecomposition;
import org.apache.commons.math3.linear.DecompositionSolver;
import org.apache.commons.math3.linear.DiagonalMatrix;
import org.apache.commons.math3.linear.EigenDecomposition;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.QRDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.SingularValueDecomposition;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameContent;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.util.LazyValue;

/**
 * An implementation of the DataFrameAlgebra interface that uses Apache Commons Math
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @see <a href="http://commons.apache.org/proper/commons-math/">Apache Commons Math</a>
 * @author  Xavier Witdouck
 */
class XDataFrameAlgebraApache<R,C> extends XDataFrameAlgebra<R,C> {

    /**
     * Constructor
     * @param frame     the frame reference
     */
    XDataFrameAlgebraApache(DataFrame<R,C> frame) {
        super(frame);
    }


    @Override
    public Decomposition decomp() {
        return new Decomp(frame());
    }


    @Override
    public DataFrame<Integer,Integer> inverse() throws DataFrameException {
        try {
            final DataFrame<R,C> frame = frame();
            final double[] ones = IntStream.range(0, frame.rowCount()).mapToDouble(i -> 1d).toArray();
            final RealMatrix identity = new DiagonalMatrix(ones);
            if (frame().rowCount() == frame().colCount()) {
                final LUDecomposition decomposition = new LUDecomposition(toMatrix(frame()));
                final DecompositionSolver solver = decomposition.getSolver();
                final RealMatrix result = solver.solve(identity);
                return toDataFrame(result);
            } else {
                final QRDecomposition decomposition = new QRDecomposition(toMatrix(frame()));
                final DecompositionSolver solver = decomposition.getSolver();
                final RealMatrix result = solver.solve(identity);
                return toDataFrame(result);
            }
        } catch (Exception ex) {
            throw new DataFrameException("Failed to compute inverse of DataFrame", ex);
        }
    }


    @Override
    public DataFrame<Integer,Integer> solve(DataFrame rhs) throws DataFrameException {
        try {
            if (frame().rowCount() == frame().colCount()) {
                final LUDecomposition decomposition = new LUDecomposition(toMatrix(frame()));
                final DecompositionSolver solver = decomposition.getSolver();
                final RealMatrix b = toMatrix(rhs);
                final RealMatrix result = solver.solve(b);
                return toDataFrame(result);
            } else {
                final QRDecomposition decomposition = new QRDecomposition(toMatrix(frame()));
                final DecompositionSolver solver = decomposition.getSolver();
                final RealMatrix b = toMatrix(rhs);
                final RealMatrix result = solver.solve(b);
                return toDataFrame(result);
            }
        } catch (Exception ex) {
            throw new DataFrameException("Failed to solve AX=B of DataFrames", ex);
        }
    }


    /**
     * Returns a newly created DataFrame from the Apache matrix
     * @param matrix    the matrix input
     * @return          the newly created DataFrame
     */
    private DataFrame<Integer,Integer> toDataFrame(RealMatrix matrix) {
        final Range<Integer> rowKeys = Range.of(0, matrix.getRowDimension());
        final Range<Integer> colKeys = Range.of(0, matrix.getColumnDimension());
        return DataFrame.ofDoubles(rowKeys, colKeys, v -> {
            final int i = v.rowOrdinal();
            final int j = v.colOrdinal();
            return matrix.getEntry(i, j);
        });
    }


    /**
     * Returns a light-weight Apache Matrix wrapper of the DataFrame
     * @param frame     the frame to wrap in an Apache Matrix interface
     * @return          the resulting Apache matrix
     */
    private RealMatrix toMatrix(DataFrame<?,?> frame) {
        final DataFrameContent<?,?> data = frame.data();
        return new AbstractRealMatrix() {
            @Override
            public int getRowDimension() {
                return frame.rowCount();
            }
            @Override
            public int getColumnDimension() {
                return frame.colCount();
            }
            @Override
            public RealMatrix createMatrix(int rowCount, int colCount) throws NotStrictlyPositiveException {
                return new Array2DRowRealMatrix(rowCount, colCount);
            }
            @Override
            public RealMatrix copy() {
                return toMatrix(frame.copy());
            }
            @Override
            public double getEntry(int rowIndex, int colIndex) throws OutOfRangeException {
                return data.getDouble(rowIndex, colIndex);
            }
            @Override
            public void setEntry(int rowIndex, int colIndex, double value) throws OutOfRangeException {
                data.setDouble(rowIndex, colIndex, value);
            }
        };
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
        private XLUD(RealMatrix matrix) {
            this.lud = new LUDecomposition(matrix);
            this.l = LazyValue.of(() -> toDataFrame(lud.getL()));
            this.u = LazyValue.of(() -> toDataFrame(lud.getU()));
            this.p = LazyValue.of(() -> toDataFrame(lud.getP()));
        }

        @Override
        public double det() {
            return lud.getDeterminant();
        }

        @Override
        public boolean isNonSingular() {
            return lud.getSolver().isNonSingular();
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
            final RealMatrix b = toMatrix(rhs);
            final RealMatrix result = lud.getSolver().solve(b);
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
        XQRD(RealMatrix matrix) {
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
            final RealMatrix b = toMatrix(rhs);
            final RealMatrix result = qrd.getSolver().solve(b);
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
        XEVD(RealMatrix matrix) {
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
        XSVD(RealMatrix x) {
            final SingularValueDecomposition svd = new SingularValueDecomposition(x);
            this.rank = svd.getRank();
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
        XCD(RealMatrix matrix) {
            this.cd = new CholeskyDecomposition(matrix);
            this.l = LazyValue.of(() -> toDataFrame(cd.getL()));
        }

        @Override
        public DataFrame<Integer,Integer> getL() {
            return l.get();
        }

        @Override
        public DataFrame<Integer,Integer> solve(DataFrame<?,?> rhs) {
            final RealMatrix b = toMatrix(rhs);
            final RealMatrix result = cd.getSolver().solve(b);
            return toDataFrame(result);
        }
    }

}



