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

import java.time.LocalDate;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameCursor;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.range.Range;

/**
 * Direct tests of the the XDataFrameContent class
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class ContentTests {


    @Test()
    public void testTranspose() {
        final Index<LocalDate> rowKeys = Range.ofLocalDates("2000-01-01", "2005-01-01").toIndex(LocalDate.class);
        final Index<String> colKeys = Index.of(Array.of("C1", "C2", "C3", "C4", "C5"));
        final XDataFrame<LocalDate,String> frame = (XDataFrame<LocalDate,String>)DataFrame.ofDoubles(rowKeys, colKeys);
        final XDataFrame<String,LocalDate> transpose = (XDataFrame<String,LocalDate>)frame.transpose();
        final DataFrameCursor<LocalDate,String> cursor0 = frame.cursor();
        final DataFrameCursor<String,LocalDate> cursor1 = transpose.cursor();
        for (int i=0; i<frame.rowCount(); ++i) {
            for (int j=0; j<frame.colCount(); ++j) {
                cursor0.atOrdinals(i, j);
                cursor1.atOrdinals(j, i);
                cursor0.setDouble(Math.random() * 10);
                Assert.assertEquals(cursor0.rowKey(), frame.rows().key(i), "Row keys match at " + i);
                Assert.assertEquals(cursor0.colKey(), frame.cols().key(j), "Column keys match at " + j);
                Assert.assertEquals(cursor0.rowKey(), cursor1.colKey(), "Transpose keys match " + i);
                Assert.assertEquals(cursor0.colKey(), cursor1.rowKey(), "Transpose keys match " + j);
                Assert.assertEquals(cursor0.getDouble(), cursor1.getDouble(), "Transpose keys match " + j);
            }
        }
    }

}
