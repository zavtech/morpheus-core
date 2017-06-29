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

import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.QRDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.array.ArrayType;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameAlgebra;
import com.zavtech.morpheus.frame.DataFrameValue;
import com.zavtech.morpheus.range.Range;

/**
 * Unit tests for DataFrame Linear Algebra functionality
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class AlgebraTests {

    /**
     * Returns a DataFrame of random values
     * @param rowCount      the row count
     * @param colCount      the column count
     * @param parallel      true for parallel version
     * @return              the random DataFrame
     */
    private DataFrame<Integer,Integer> random(int rowCount, int colCount, boolean parallel, Class<?> type) {
        final Random random = new Random();
        final Range<Integer> rowKeys = Range.of(0, rowCount);
        final Range<Integer> colKeys = Range.of(0, colCount);
        if (type == int.class || type == Integer.class) {
            final DataFrame<Integer,Integer> frame = DataFrame.ofInts(rowKeys, colKeys, v -> random.nextInt());
            return parallel ? frame.parallel() : frame.sequential();
        } else if (type == long.class || type == Long.class) {
            final DataFrame<Integer,Integer> frame = DataFrame.ofLongs(rowKeys, colKeys, v -> random.nextLong());
            return parallel ? frame.parallel() : frame.sequential();
        } else if (type == double.class || type == Double.class) {
            final DataFrame<Integer,Integer> frame = DataFrame.ofDoubles(rowKeys, colKeys, v -> random.nextDouble() * 100);
            return parallel ? frame.parallel() : frame.sequential();
        } else {
            throw new IllegalArgumentException("Unsupported type specified: " + type);
        }
    }


    /**
     * Returns an Apache matrix representation of the DataFrame
     * @param frame     the DataFrame reference
     * @return          the Apache RealMatrix
     */
    private RealMatrix toMatrix(DataFrame<?,?> frame) {
        RealMatrix matrix = new Array2DRowRealMatrix(frame.rowCount(), frame.colCount());
        frame.forEachValue(v -> {
            final int i = v.rowOrdinal();
            final int j = v.colOrdinal();
            final double value = v.getDouble();
            matrix.setEntry(i, j, value);
        });
        return matrix;
    }

    /**
     * Asserts the the dimenions and elements values of the two args match
     * @param frame     the frame reference for comparison
     * @param matrix    the matrix reference for comparison
     */
    void assertEquals(DataFrame<?,?> frame, RealMatrix matrix) {
        Assert.assertEquals(frame.rowCount(), matrix.getRowDimension());
        Assert.assertEquals(frame.colCount(), matrix.getColumnDimension());
        frame.forEachValue(v -> {
            final int i = v.rowOrdinal();
            final int j = v.colOrdinal();
            final double actual = v.getDouble();
            final double expected = matrix.getEntry(i, j);
            final double delta = Math.max(0.0000001, Math.abs(expected * 0.000000001));
            Assert.assertEquals(actual, expected, delta, String.format("Values match at coordinates (%s,%s)", i, j));
        });
    }


    /**
     * Asserts the the dimenions and elements values of the two args match
     * @param frame     the frame reference for comparison
     */
    void assertEquals(DataFrame<?,?> frame, Function<DataFrameValue<?,?>,Number> expected) {
        frame.forEachValue(v -> {
            final int i = v.rowOrdinal();
            final int j = v.colOrdinal();
            final double actValue = v.getDouble();
            final double expValue = expected.apply(v).doubleValue();
            final double delta = Math.max(0.0000001, Math.abs(expValue * 0.000000001));
            Assert.assertEquals(actValue, expValue, delta, String.format("Values match at coordinates (%s,%s)", i, j));
        });
    }



    @DataProvider(name="styles")
    public Object[][] styles() {
        return new Object[][] {
            { DataFrameAlgebra.Lib.JAMA, true },
            { DataFrameAlgebra.Lib.APACHE, true },
            { DataFrameAlgebra.Lib.JAMA, false },
            { DataFrameAlgebra.Lib.APACHE, false }
        };
    }


    @Test(dataProvider = "styles")
    public void testPlusScalar(DataFrameAlgebra.Lib lib, boolean parallel) {
        DataFrameAlgebra.LIBRARY.set(lib);
        Stream.of(int.class, long.class, double.class).forEach(type -> {
            Array.of(20, 77, 95, 135, 233, 245).forEach(count -> {
                final DataFrame<Integer,Integer> frame = random(120, count, parallel, type);
                final DataFrame<Integer,Integer> result = frame.plus(25);
                assertEquals(result, v -> {
                    switch (ArrayType.of(type)) {
                        case INTEGER:   return frame.data().getInt(v.rowOrdinal(), v.colOrdinal()) + 25;
                        case LONG:      return frame.data().getLong(v.rowOrdinal(), v.colOrdinal()) + 25;
                        default:        return frame.data().getDouble(v.rowOrdinal(), v.colOrdinal()) + 25d;
                    }
                });
            });
        });
    }


    @Test(dataProvider = "styles")
    public void testPlusFrame(DataFrameAlgebra.Lib lib, boolean parallel) {
        DataFrameAlgebra.LIBRARY.set(lib);
        Stream.of(int.class, long.class, double.class).forEach(type -> {
            Array.of(20, 77, 95, 135, 233, 245).forEach(count -> {
                final DataFrame<Integer,Integer> left = random(120, count, parallel, type);
                final DataFrame<Integer,Integer> right = random(120, count, parallel, type);
                final DataFrame<Integer,Integer> result = left.plus(right);
                assertEquals(result, v -> {
                    final int i = v.rowOrdinal();
                    final int j = v.colOrdinal();
                    switch (ArrayType.of(type)) {
                        case INTEGER:   return left.data().getInt(i,j) + right.data().getInt(i,j);
                        case LONG:      return left.data().getLong(i,j) + right.data().getLong(i,j);
                        default:        return left.data().getDouble(i,j) + right.data().getDouble(i,j);
                    }
                });
            });
        });
    }


    @Test(dataProvider = "styles")
    public void testMinusScalar(DataFrameAlgebra.Lib lib, boolean parallel) {
        DataFrameAlgebra.LIBRARY.set(lib);
        Stream.of(int.class, long.class, double.class).forEach(type -> {
            Array.of(20, 77, 95, 135, 233, 245).forEach(count -> {
                final DataFrame<Integer,Integer> frame = random(120, count, parallel, type);
                final DataFrame<Integer,Integer> result = frame.minus(25);
                assertEquals(result, v -> {
                    switch (ArrayType.of(type)) {
                        case INTEGER:   return frame.data().getInt(v.rowOrdinal(), v.colOrdinal()) - 25;
                        case LONG:      return frame.data().getLong(v.rowOrdinal(), v.colOrdinal()) - 25;
                        default:        return frame.data().getDouble(v.rowOrdinal(), v.colOrdinal()) - 25d;
                    }
                });
            });
        });
    }


    @Test(dataProvider = "styles")
    public void testMinusFrame(DataFrameAlgebra.Lib lib, boolean parallel) {
        DataFrameAlgebra.LIBRARY.set(lib);
        Stream.of(int.class, long.class, double.class).forEach(type -> {
            Array.of(20, 77, 95, 135, 233, 245).forEach(count -> {
                final DataFrame<Integer,Integer> left = random(120, count, parallel, type);
                final DataFrame<Integer,Integer> right = random(120, count, parallel, type);
                final DataFrame<Integer,Integer> result = left.minus(right);
                assertEquals(result, v -> {
                    final int i = v.rowOrdinal();
                    final int j = v.colOrdinal();
                    switch (ArrayType.of(type)) {
                        case INTEGER:   return left.data().getInt(i,j) - right.data().getInt(i,j);
                        case LONG:      return left.data().getLong(i,j) - right.data().getLong(i,j);
                        default:        return left.data().getDouble(i,j) - right.data().getDouble(i,j);
                    }
                });
            });
        });
    }


    @Test(dataProvider = "styles")
    public void testTimesScalar(DataFrameAlgebra.Lib lib, boolean parallel) {
        DataFrameAlgebra.LIBRARY.set(lib);
        Stream.of(int.class, long.class, double.class).forEach(type -> {
            Array.of(20, 77, 95, 135, 233, 245).forEach(count -> {
                final DataFrame<Integer,Integer> frame = random(120, count, parallel, type);
                final DataFrame<Integer,Integer> result = frame.times(25);
                assertEquals(result, v -> {
                    switch (ArrayType.of(type)) {
                        case INTEGER:   return frame.data().getInt(v.rowOrdinal(), v.colOrdinal()) * 25;
                        case LONG:      return frame.data().getLong(v.rowOrdinal(), v.colOrdinal()) * 25;
                        default:        return frame.data().getDouble(v.rowOrdinal(), v.colOrdinal()) * 25d;
                    }
                });
            });
        });
    }


    @Test(dataProvider = "styles")
    public void testTimesFrame(DataFrameAlgebra.Lib lib, boolean parallel) {
        DataFrameAlgebra.LIBRARY.set(lib);
        Stream.of(int.class, long.class, double.class).forEach(type -> {
            Array.of(20, 77, 95, 135, 233, 245).forEach(count -> {
                final DataFrame<Integer,Integer> left = random(120, count, parallel, type);
                final DataFrame<Integer,Integer> right = random(120, count, parallel, type);
                final DataFrame<Integer,Integer> result = left.times(right);
                assertEquals(result, v -> {
                    final int i = v.rowOrdinal();
                    final int j = v.colOrdinal();
                    switch (ArrayType.of(type)) {
                        case INTEGER:   return left.data().getInt(i,j) * right.data().getInt(i,j);
                        case LONG:      return left.data().getLong(i,j) * right.data().getLong(i,j);
                        default:        return left.data().getDouble(i,j) * right.data().getDouble(i,j);
                    }
                });
            });
        });
    }


    @Test(dataProvider = "styles")
    public void testDivideScalar(DataFrameAlgebra.Lib lib, boolean parallel) {
        DataFrameAlgebra.LIBRARY.set(lib);
        Stream.of(int.class, long.class, double.class).forEach(type -> {
            Array.of(20, 77, 95, 135, 233, 245).forEach(count -> {
                final DataFrame<Integer,Integer> frame = random(120, count, parallel, type);
                final DataFrame<Integer,Integer> result = frame.divide(25);
                assertEquals(result, v -> {
                    switch (ArrayType.of(type)) {
                        case INTEGER:   return frame.data().getInt(v.rowOrdinal(), v.colOrdinal()) / 25;
                        case LONG:      return frame.data().getLong(v.rowOrdinal(), v.colOrdinal()) / 25;
                        default:        return frame.data().getDouble(v.rowOrdinal(), v.colOrdinal()) / 25d;
                    }
                });
            });
        });
    }


    @Test(dataProvider = "styles")
    public void testDivideFrame(DataFrameAlgebra.Lib lib, boolean parallel) {
        DataFrameAlgebra.LIBRARY.set(lib);
        Stream.of(int.class, long.class, double.class).forEach(type -> {
            Array.of(20, 77, 95, 135, 233, 245).forEach(count -> {
                final DataFrame<Integer,Integer> left = random(120, count, parallel, type);
                final DataFrame<Integer,Integer> right = random(120, count, parallel, type);
                final DataFrame<Integer,Integer> result = left.divide(right);
                assertEquals(result, v -> {
                    final int i = v.rowOrdinal();
                    final int j = v.colOrdinal();
                    switch (ArrayType.of(type)) {
                        case INTEGER:   return left.data().getInt(i,j) / right.data().getInt(i,j);
                        case LONG:      return left.data().getLong(i,j) / right.data().getLong(i,j);
                        default:        return left.data().getDouble(i,j) / right.data().getDouble(i,j);
                    }
                });
            });
        });
    }


    @Test(dataProvider = "styles")
    public void testDotProduct(DataFrameAlgebra.Lib lib, boolean parallel) {
        DataFrameAlgebra.LIBRARY.set(lib);
        Array.of(20, 77, 95, 135, 233, 245).forEach(count -> {
            final DataFrame<Integer,Integer> left = random(120, count, parallel, double.class);
            final DataFrame<Integer,Integer> right = random(count, 50, parallel, double.class);
            final DataFrame<Integer,Integer> result = left.dot(right);
            Assert.assertEquals(result.rowCount(), left.rowCount());
            Assert.assertEquals(result.colCount(), right.colCount());
            final RealMatrix matrix = toMatrix(left).multiply(toMatrix(right));
            assertEquals(result, matrix);
        });
    }

    /*
    @Test(dataProvider = "styles")
    public void testDeterminant(DataFrameAlgebra.Lib lib, boolean parallel) {
        DataFrameAlgebra.LIBRARY.set(lib);
        final DataFrame<Integer,String> source = DataFrame.read().csv("./src/test/resources/pca/svd/poppet-svd-eigenvectors.csv");
        final DataFrame<Integer,String> frame = parallel ? source.parallel() : source.sequential();
        final double det = new LUDecomposition(toMatrix(frame)).getDeterminant();
        Assert.assertEquals(Math.abs(frame.det()), Math.abs(det), 0.00001);
    }
    */


    @Test(dataProvider = "styles")
    public void testInverse(DataFrameAlgebra.Lib lib, boolean parallel) {
        DataFrameAlgebra.LIBRARY.set(lib);
        Array.of(20, 77, 95, 135, 233, 245).forEach(count -> {
            final DataFrame<Integer,Integer> frame = random(count, count, parallel, double.class);
            final DataFrame<Integer,Integer> inverse = frame.inverse();
            final RealMatrix matrix = new LUDecomposition(toMatrix(frame)).getSolver().getInverse();
            assertEquals(inverse, matrix);
        });
    }


    @Test(dataProvider = "styles")
    public void testPseudoInverse(DataFrameAlgebra.Lib lib, boolean parallel) {
        DataFrameAlgebra.LIBRARY.set(lib);
        final DataFrame<Integer,String> source = DataFrame.read().csv("./src/test/resources/pca/svd/poppet-svd-eigenvectors.csv");
        Array.of(20, 77, 95, 135, 233, 245).forEach(count -> {
            final DataFrame<Integer,String> frame = source.cols().select(col -> col.ordinal() < count);
            final DataFrame<Integer,Integer> inverse = frame.inverse();
            final RealMatrix matrix = new QRDecomposition(toMatrix(frame)).getSolver().getInverse();
            assertEquals(inverse, matrix);
        });
    }

}
