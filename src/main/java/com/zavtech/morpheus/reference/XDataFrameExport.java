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

import com.zavtech.morpheus.frame.DataFrameExport;
import com.zavtech.morpheus.jama.Matrix;

import org.apache.commons.math3.exception.NotStrictlyPositiveException;
import org.apache.commons.math3.exception.OutOfRangeException;
import org.apache.commons.math3.linear.AbstractRealMatrix;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;

/**
 * The reference implementation of the DataFrameExport interface
 *
 * @param <R>   the row key type
 * @param <C>   the column key type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class XDataFrameExport<R,C> implements DataFrameExport {

    private XDataFrame<R,C> frame;

    /**
     * Constructor
     * @param frame the frame to export
     */
    XDataFrameExport(XDataFrame<R,C> frame) {
        this.frame = frame;
    }


    @Override
    public final Matrix asMatrix() {
        final Matrix matrix = new Matrix(frame.rowCount(), frame.colCount());
        frame.forEachValue(v -> matrix.set(v.rowOrdinal(), v.colOrdinal(), v.getDouble()));
        return matrix;
    }



    @Override
    public final RealMatrix asApacheMatrix() {
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
                return frame.copy().export().asApacheMatrix();
            }
            @Override
            public double getEntry(int rowIndex, int colIndex) throws OutOfRangeException {
                return frame.data().getDouble(rowIndex, colIndex);
            }
            @Override
            public void setEntry(int rowIndex, int colIndex, double value) throws OutOfRangeException {
                frame.data().setDouble(rowIndex, colIndex, value);
            }
        };
    }
}
