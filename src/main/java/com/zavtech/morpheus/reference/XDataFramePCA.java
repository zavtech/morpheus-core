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
package com.zavtech.morpheus.reference;

import java.util.Optional;
import java.util.function.Function;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.SingularValueDecomposition;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFramePCA;
import com.zavtech.morpheus.jama.EigenDecomposition;
import com.zavtech.morpheus.jama.Matrix;
import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.stats.StatType;
import com.zavtech.morpheus.util.Asserts;
import com.zavtech.morpheus.util.IntComparator;
import com.zavtech.morpheus.util.SortAlgorithm;
import com.zavtech.morpheus.util.Swapper;

/**
 * The default implementation of the DataFramePCA interface to perform Principal Component Analysis on the columns of a DataFrame
 *
 * @param <R>   the frame row key type
 * @param <C>   the frame column key type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class XDataFramePCA<R,C> implements DataFramePCA<R,C> {

    private XDataFrame<R,C> frame;

    /**
     * Constructor
     * @param frame     the frame reference
     */
    XDataFramePCA(XDataFrame<R,C> frame) {
        this.frame = frame;
    }


    @Override
    public <T> Optional<T> apply(boolean demean, Function<Model<R,C>, Optional<T>> handler) {
        return apply(demean, Solver.SVD, handler);
    }


    @Override
    public <T> Optional<T> apply(boolean demean, Solver solver, Function<Model<R, C>, Optional<T>> handler) {
        Asserts.notNull(solver, "The PCA Solver cannot be null");
        Asserts.notNull(handler, "The PCA lambda handler cannot be null");
        switch (solver) {
            case SVD:           return handler.apply(new ModelWithSVD<>(frame, demean));
            case EVD_COV:       return handler.apply(new ModelWithEVD<>(frame, demean, true));
            case EVD_COR:       return handler.apply(new ModelWithEVD<>(frame, demean, false));
            default:            throw new DataFrameException("Unsupported PCA solver specified: " + solver);
        }
    }



    /**
     * An implementation of a PCA model that uses SingularValueDecomposition
     */
    private class ModelWithSVD<X,Y> extends ModelBase<X,Y> {

        /**
         * Constructor
         * @param data      the data frame reference
         * @param demean    true if the frame columns should be demeaned
         */
        private ModelWithSVD(XDataFrame<X,Y> data, boolean demean) {
            super(data, demean);
        }

        @Override()
        boolean calculate() {
            if (!isDirty()) {
                return false;
            } else if (data().colCount() > data().rowCount()) {
                throw new DataFrameException("PCA Analysis expects frame as nxp matrix where n>=p, transpose and try again");
            } else {
                try {
                    final double rowCount = data().rowCount();
                    final long t1 = System.currentTimeMillis();
                    final RealMatrix matrix = data().export().asApacheMatrix();
                    final SingularValueDecomposition svd = new SingularValueDecomposition(matrix);
                    final DoubleStream singularValues = DoubleStream.of(svd.getSingularValues());
                    final double[] eigenValues = singularValues.map(v -> Math.pow(v, 2d) / (rowCount - 1d)).toArray();
                    this.update(eigenValues, svd.getV());
                    final long t2 = System.currentTimeMillis();
                    System.out.println("SVD Decomposition completed in " + (t2-t1) + " millis");
                    return true;
                } catch (Exception ex) {
                    throw new DataFrameException("Failed to perform SVD on input data for PCA", ex);
                }
            }
        }
    }


    /**
     * An implementation of a PCA model that uses EigenValueDecomposition
     */
    private class ModelWithEVD<X,Y> extends ModelBase<X,Y> {

        private boolean cov;

        /**
         * Constructor
         * @param frame     the data frame reference
         * @param demean    true if the frame columns should be demeaned
         * @param cov       true to use covariance matrix, false for correlation matrix
         */
        private ModelWithEVD(XDataFrame<X,Y> frame, boolean demean, boolean cov) {
            super(frame, demean);
            this.cov = cov;
        }


        @Override()
        boolean calculate() {
            if (!isDirty()) {
                return false;
            } else if (data().colCount() > data().rowCount()) {
                throw new DataFrameException("PCA Analysis expects frame as nxp matrix where n>=p, transpose and try again");
            } else {
                try {
                    final long t1 = System.currentTimeMillis();
                    final DataFrame<Y,Y> input = cov ? data().cols().stats().covariance() : data().cols().stats().correlation();
                    final Matrix x = input.export().asMatrix();
                    final EigenDecomposition evd = new EigenDecomposition(x);
                    final double[] eigenValues = evd.getRealEigenvalues();
                    this.update(eigenValues, evd.getV());
                    final long t2 = System.currentTimeMillis();
                    System.out.println("Eigen Decomposition completed in " + (t2-t1) + " millis");
                    return true;
                } catch (Exception ex) {
                    throw new DataFrameException("Failed to perform SVD on input data for PCA", ex);
                }
            }
        }
    }


    /**
     * A convenience base class for building various models to implement Principal Component Analysis
     */
    private abstract class ModelBase<X,Y> implements Model<X,Y> {

        private DataFrame<X,Y> data;
        private DataFrame<Y,StatType> means;
        private DataFrame<Integer,Field> eigenValues;
        private DataFrame<Integer,Integer> eigenVectors;

        /**
         * Constructor
         * @param data      the data frame reference
         * @param demean    true if the frame columns should be demeaned
         */
        ModelBase(DataFrame<X,Y> data, boolean demean) {
            this.data = data;
            if (demean) {
                this.means = data.cols().stats().mean();
                this.data = data.cols().demean(false);
            }
        }

        /**
         * Triggers this model to calculate if it is dirty
         * @return  true if calculation performed, false if was not necessary
         */
        abstract boolean calculate();


        /**
         * Returns the data for this model, which could be demeaned version of raw data
         * @return      the data for this model
         */
        DataFrame<X,Y> data() {
            return data;
        }

        /**
         * Returns true if this model demeans the columns of the input DataFrame
         * @return  true if the columns are demeaned as part of the PCA
         */
        public boolean isDemean() {
            return means != null;
        }

        /**
         * Returns true if this model is dirty and needs to be re-computed
         * @return      true if this model is dirty
         */
        boolean isDirty() {
            return eigenValues == null;
        }

        /**
         * Called by a subclass in order to present the eigenvalues and eigenvectors generated by the relevant decomposition
         * @param eigenValues       the array of eigen values
         * @param eigenVectors      the matrix of eigen vectors expressed as columns
         */
        protected void update(double[] eigenValues, Matrix eigenVectors) {
            final Ordering ordering = new Ordering(eigenValues);
            SortAlgorithm.getDefault(false).sort(0, eigenValues.length, ordering, ordering);
            final int[] indices = ordering.getIndices();
            final Range<Integer> rowKeys = Range.of(0, eigenValues.length);
            this.eigenValues = DataFrame.ofDoubles(rowKeys, Array.of(Field.EIGENVALUE));
            this.eigenValues.applyDoubles(v -> eigenValues[indices[v.rowOrdinal()]]);
            this.addVariancePercentages();
            this.eigenVectors = DataFrame.of(Range.of(0, eigenValues.length), Integer.class, columns -> {
                for (int i = 0; i < indices.length; ++i) {
                    final int index = indices[i];
                    columns.add(i, Array.of(Double.class, eigenValues.length).applyDoubles(v -> {
                        return eigenVectors.get(v.index(), index);
                    }));
                }
            });
        }


        /**
         * Called by a subclass in order to present the eigenvalues and eigenvectors generated by the relevant decomposition
         * @param eigenValues       the array of eigen values
         * @param eigenVectors      the matrix of eigen vectors expressed as columns
         */
        protected void update(double[] eigenValues, RealMatrix eigenVectors) {
            final Ordering ordering = new Ordering(eigenValues);
            SortAlgorithm.getDefault(false).sort(0, eigenValues.length, ordering, ordering);
            final int[] indices = ordering.getIndices();
            final Range<Integer> rowKeys = Range.of(0, eigenValues.length);
            this.eigenValues = DataFrame.ofDoubles(rowKeys, Array.of(Field.EIGENVALUE));
            this.eigenValues.applyDoubles(v -> eigenValues[indices[v.rowOrdinal()]]);
            this.addVariancePercentages();
            this.eigenVectors = DataFrame.of(Range.of(0, eigenValues.length), Integer.class, columns -> {
                for (int i = 0; i < indices.length; ++i) {
                    final int index = indices[i];
                    columns.add(i, Array.of(Double.class, eigenValues.length).applyDoubles(v -> {
                         return eigenVectors.getEntry(v.index(), index);
                    }));
                }
            });
        }

        /**
         * Adds two columns to the eigenvalue data frame, one for percent of variance, and for cumulative percent of variance
         */
        private void addVariancePercentages() {
            final double sum = eigenValues.col(Field.EIGENVALUE).stats().sum();
            this.eigenValues.cols().add(Field.VAR_PERCENT, Double.class, v -> v.row().getDouble(Field.EIGENVALUE) / sum);
            this.eigenValues.cols().add(Field.VAR_PERCENT_CUM, Double.class, v -> {
                switch (v.rowOrdinal()) {
                    case 0: return v.row().getDouble(Field.VAR_PERCENT);
                    default:
                        final double prior = v.col().getDouble(v.rowOrdinal()-1);
                        final double current = v.row().getDouble(Field.VAR_PERCENT);
                        return current + prior;
                }
            });
        }


        @Override
        public DataFrame<Integer,Integer> getEigenVectors() {
            this.calculate();
            return eigenVectors;
        }


        @Override
        public DataFrame<Integer,Field> getEigenValues() {
            this.calculate();
            return eigenValues;
        }


        @Override
        public DataFrame<X,Integer> getScores() {
            this.calculate();
            return getScores(eigenValues.rowCount());
        }


        @Override
        public DataFrame<X,Y> getProjection(int numComponents) {
            this.calculate();
            final DataFrame<X,Integer> scores = getScores(numComponents);   //nxk
            final DataFrame<Integer,Integer> basis = eigenVectors.cols().select(col -> col.ordinal() < numComponents);  //pxk
            final Matrix basisMatrix = basis.transpose().export().asMatrix();
            final Matrix scoreMatrix = scores.export().asMatrix();
            final Matrix result = scoreMatrix.times(basisMatrix);
            if (!isDemean()) {
                return data.mapToDoubles(v -> result.get(v.rowOrdinal(), v.colOrdinal()));
            } else {
                return data.mapToDoubles(v -> {
                    final double value = result.get(v.rowOrdinal(), v.colOrdinal());
                    final double mean = means.data().getDouble(v.colOrdinal(), 0);
                    return value + mean;
                });
            }
        }


        @Override
        public DataFrame<X,Integer> getScores(int numComponents) {
            this.calculate();
            final DataFrame<Integer,Integer> V = eigenVectors.cols().select(col -> col.ordinal() < numComponents);
            final Matrix original = data.export().asMatrix();   // nxp
            final Matrix basis = V.export().asMatrix();         // pxk
            final Matrix scores = original.times(basis);        // nxk
            final Array<X> rowKeys = data().rows().keyArray();
            final Range<Integer> colKeys = Range.of(0, numComponents);
            return DataFrame.ofDoubles(rowKeys, colKeys, v -> {
                return scores.get(v.rowOrdinal(), v.colOrdinal());
            });
        }
    }



    /**
     * A class that sorts an array of indices that yield the desired order of the input array
     */
    private class Ordering implements Swapper, IntComparator {

        private double[] values;
        private int[] indices;

        /**
         * Constructor
         * @param values    the values to generate a sorted index
         */
        Ordering(double[] values) {
            this.values = values;
            this.indices = IntStream.range(0, values.length).toArray();
        }

        /**
         * Returns the indices with the desired ordering
         * @return  the ordered indices
         */
        int[] getIndices() {
            return indices;
        }

        @Override
        public int compare(int index1, int index2) {
            return -1 * Double.compare(values[indices[index1]], values[indices[index2]]);
        }

        @Override
        public void swap(int index1, int index2) {
            final int i1 = indices[index1];
            final int i2 = indices[index2];
            this.indices[index1] = i2;
            this.indices[index2] = i1;
        }
    }
}
