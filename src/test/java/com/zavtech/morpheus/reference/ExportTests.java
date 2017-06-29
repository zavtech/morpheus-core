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

import com.zavtech.morpheus.frame.DataFrame;
import org.apache.commons.math3.linear.RealMatrix;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for the DataFrameExport interface
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class ExportTests {



    @Test()
    public void testRealMatrixRead() {
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 100, 100);
        final RealMatrix matrix = frame.export().asApacheMatrix();
        Assert.assertEquals(frame.rowCount(), matrix.getRowDimension(), "Row count matches");
        Assert.assertEquals(frame.colCount(), matrix.getColumnDimension(), "Column count matches");
        for (int i=0; i<frame.rowCount(); ++i) {
            for (int j = 0; j<frame.colCount(); ++j) {
                final double v1 = frame.data().getDouble(i, j);
                final double v2 = matrix.getEntry(i, j);
                Assert.assertEquals(v1, v2, "Values match at " + i + "," + j);
            }
        }
    }


    @Test()
    public void testRealMatrixReadAfterModify() {
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 100, 100);
        final RealMatrix matrix = frame.export().asApacheMatrix();
        Assert.assertEquals(frame.rowCount(), matrix.getRowDimension(), "Row count matches");
        Assert.assertEquals(frame.colCount(), matrix.getColumnDimension(), "Column count matches");
        frame.applyDoubles(v -> Math.random());
        for (int i=0; i<frame.rowCount(); ++i) {
            for (int j = 0; j<frame.colCount(); ++j) {
                final double v1 = frame.data().getDouble(i, j);
                final double v2 = matrix.getEntry(i, j);
                Assert.assertEquals(v1, v2, "Values match at " + i + "," + j);
            }
        }
    }


    @Test()
    public void testRealMatrixUpdates() {
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 100, 100);
        final RealMatrix matrix = frame.export().asApacheMatrix();
        Assert.assertEquals(frame.rowCount(), matrix.getRowDimension(), "Row count matches");
        Assert.assertEquals(frame.colCount(), matrix.getColumnDimension(), "Column count matches");
        for (int i=0; i<frame.rowCount(); ++i) {
            for (int j = 0; j<frame.colCount(); ++j) {
                matrix.setEntry(i, j, Math.random());
            }
        }
        for (int i=0; i<frame.rowCount(); ++i) {
            for (int j = 0; j<frame.colCount(); ++j) {
                final double v1 = frame.data().getDouble(i, j);
                final double v2 = matrix.getEntry(i, j);
                Assert.assertEquals(v1, v2, "Values match at " + i + "," + j);
            }
        }
    }


    @Test()
    public void testRealMatrixCopy() {
        final DataFrame<String,String> frame = TestDataFrames.random(double.class, 100, 100);
        final RealMatrix matrix = frame.export().asApacheMatrix().copy();
        frame.applyDoubles(v -> Math.random() * 10);
        for (int i=0; i<frame.rowCount(); ++i) {
            for (int j = 0; j<frame.colCount(); ++j) {
                final double v1 = frame.data().getDouble(i, j);
                final double v2 = matrix.getEntry(i, j);
                Assert.assertTrue(v1 != v2, "Values do not match at " + i + "," + j);
            }
        }
    }

}
