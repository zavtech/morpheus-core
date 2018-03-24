package com.zavtech.morpheus.array;

import com.zavtech.morpheus.frame.DataFrame;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

import java.time.Month;

/**
 * Unit tests for the array concat and expand functionalities
 *
 * @author  Manoel Campos da Silva Filho
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class ArrayConcatTest {
    private final DataFrame<Integer, Month> df1 = DataFrame.ofDoubles(Array.of(0), Array.of(Month.JANUARY), v -> 0);
    private final DataFrame<Integer, Month> df2 = DataFrame.ofDoubles(Array.of(1), Array.of(Month.FEBRUARY), v -> 1);
    private final DataFrame<Integer, Month> df3 = DataFrame.ofDoubles(Array.of(2), Array.of(Month.MARCH), v -> 2);
    private final DataFrame<Integer, Month> df4 = DataFrame.ofDoubles(Array.of(3), Array.of(Month.APRIL), v -> 3);

    private Array<DataFrame<Integer, Month>> a1;
    private final Array<DataFrame<Integer, Month>> a2 = Array.of(df2);
    private final Array<DataFrame<Integer, Month>> a3 = Array.of(df3);
    private final Array<DataFrame<Integer, Month>> a4 = Array.of(df4);

    @BeforeMethod
    public void setUp(){
        a1 = Array.of(df1);
    }

    @Test
    public void concatOneArrayCheckElementsAddedTest(){
        assertArrayElementEqualsTo(a1.concat(a2), df1, df2);
    }

    @Test
    public void expandLength1CheckElementsAddedTest(){
        expandArray(a1, df2);
        assertArrayElementEqualsTo(a1, df1, df2);
    }

    @Test
    public void expandLength2CheckElementsAddedTest(){
        expandArray(a1, df2, df3);
        assertArrayElementEqualsTo(a1, df1, df2, df3);
    }

    @Test
    public void concatTwoArraysCheckElementsAddedTest(){
        assertArrayElementEqualsTo(a1.concat(a2).concat(a3), df1, df2, df3);
    }

    @Test
    public void concatThreeArraysCheckElementsAddedTest(){
        assertArrayElementEqualsTo(a1.concat(a2).concat(a3).concat(a4), df1, df2, df3, df4);
    }

    private void expandArray(
            final Array<DataFrame<Integer, Month>> array,
            final DataFrame<Integer, Month> ...elements)
    {
        int i = elements.length;
        array.expand(array.length() + i);
        for (DataFrame<Integer, Month> e: elements) {
            array.setValue(array.length()-i, e);
            i--;
        }
    }

    private void assertArrayElementEqualsTo(
            final Array<DataFrame<Integer, Month>> array,
            final DataFrame<Integer, Month> ...elements)
    {
        if(array.length() != elements.length){
            throw new IllegalArgumentException("Parameters array and elements must have the same length");
        }

        final SoftAssert softAssert = new SoftAssert();

        for (int i = 0; i < array.length(); i++) {
            softAssert.assertEquals(array.getValue(i), elements[i],"Element "+i+" of the array: ");
        }

        softAssert.assertAll();
    }
}
