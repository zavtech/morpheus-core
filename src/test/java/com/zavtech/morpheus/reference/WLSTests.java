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

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameLeastSquares.Field;

/**
 * Unit tests for Weighted Least Squares Regression Analysis
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class WLSTests {

    private Object[][] wrap(Object value) {
        return new Object[][] { { value } };
    }


    @DataProvider(name = "data1")
    public Object[][] testData1() {
        return wrap(DataFrame.read().csv(options -> {
            options.setResource("/csv/supervisor.csv");
        }));
    }


    @DataProvider(name = "data2")
    public Object[][] testData2() {
        return wrap(DataFrame.read().csv(options -> {
            options.setResource("/csv/wls-2.csv");
            options.setExcludeColumnIndexes(0);
        }));
    }


    @Test(dataProvider = "data1")
    public void testSupervisorWithIntercept(DataFrame<Integer,String> data) {
        final Array<Double> weights = computeWeights(data);
        data.regress().wls("y", "x", weights, true, model -> {
            System.out.println(model);

            Assert.assertEquals(model.getBetaValue("x", Field.PARAMETER), 0.12097357, 0.0000001);
            Assert.assertEquals(model.getBetaValue("x", Field.STD_ERROR), 0.00900026, 0.0000001);
            Assert.assertEquals(model.getBetaValue("x", Field.T_STAT), 13.4411138, 0.0000001);
            Assert.assertEquals(model.getBetaValue("x", Field.P_VALUE), 0, 0.0000001);
            Assert.assertEquals(model.getBetaValue("x", Field.CI_LOWER), 0.10243718, 0.0000001);
            Assert.assertEquals(model.getBetaValue("x", Field.CI_UPPER), 0.13950996, 0.0000001);

            Assert.assertEquals(model.getInterceptValue(Field.PARAMETER), 3.8130149, 0.0000001);
            Assert.assertEquals(model.getInterceptValue(Field.STD_ERROR), 4.57531141, 0.0000001);
            Assert.assertEquals(model.getInterceptValue(Field.T_STAT), 0.83338915, 0.0000001);
            Assert.assertEquals(model.getInterceptValue(Field.P_VALUE), 0.41251711, 0.0000001);
            Assert.assertEquals(model.getInterceptValue(Field.CI_LOWER), -5.61001533, 0.0000001);
            Assert.assertEquals(model.getInterceptValue(Field.CI_UPPER), 13.23604513, 0.0000001);

            Assert.assertEquals(model.getN(), 27);
            Assert.assertEquals(model.getRSquared(), 0.8784422363997759, 0.00000001);
            Assert.assertEquals(model.getRSquaredAdj(), 0.8735799258557669, 0.00000001);
            Assert.assertEquals(model.getDurbinWatsonStatistic(), 2.4672323451687337, 0.0000001);

            return Optional.empty();
        });
    }


    @Test(dataProvider = "data1")
    public void testSupervisorWithoutIntercept(DataFrame<Integer,String> data) {
        final Array<Double> weights = computeWeights(data);
        data.regress().wls("y", "x", weights, false, model -> {

            System.out.println(model);

            Assert.assertEquals(model.getBetaValue("x", Field.PARAMETER), 0.12753409, 0.0000001);
            Assert.assertEquals(model.getBetaValue("x", Field.STD_ERROR), 0.00433719, 0.0000001);
            Assert.assertEquals(model.getBetaValue("x", Field.T_STAT), 29.40477535, 0.0000001);
            Assert.assertEquals(model.getBetaValue("x", Field.P_VALUE), 0, 0.0000001);
            Assert.assertEquals(model.getBetaValue("x", Field.CI_LOWER), 0.11860148, 0.0000001);
            Assert.assertEquals(model.getBetaValue("x", Field.CI_UPPER), 0.1364667, 0.0000001);

            Assert.assertEquals(model.getN(), 27);
            Assert.assertEquals(model.getRSquared(), 0.9708075358677386, 0.00000001);
            Assert.assertEquals(model.getRSquaredAdj(), 0.9696847487857285, 0.00000001);
            Assert.assertEquals(model.getDurbinWatsonStatistic(), 2.4495122297182377, 0.0000001);

            return Optional.empty();
        });
    }


    @Test(dataProvider = "data2")
    public void testMultipleWithIntercept(DataFrame<Integer,String> data) {
        final Array<Double> weights = computeWeightsMultiple(data);
        data.regress().wls("Y", Array.of("X1", "X2"), weights, true, model -> {

            System.out.println(model);

            Assert.assertEquals(model.getBetaValue("X1", Field.PARAMETER), 0.91376171, 0.0000001);
            Assert.assertEquals(model.getBetaValue("X1", Field.STD_ERROR), 4.88936522, 0.0000001);
            Assert.assertEquals(model.getBetaValue("X1", Field.T_STAT), 0.1868876, 0.0000001);
            Assert.assertEquals(model.getBetaValue("X1", Field.P_VALUE), 0.8521391, 0.0000001);
            Assert.assertEquals(model.getBetaValue("X1", Field.CI_LOWER), -8.79027481, 0.0000001);
            Assert.assertEquals(model.getBetaValue("X1", Field.CI_UPPER), 10.61779822, 0.0000001);

            Assert.assertEquals(model.getBetaValue("X2", Field.PARAMETER), 9.31873106, 0.0000001);
            Assert.assertEquals(model.getBetaValue("X2", Field.STD_ERROR), 4.9036057, 0.0000001);
            Assert.assertEquals(model.getBetaValue("X2", Field.T_STAT), 1.90038344, 0.0000001);
            Assert.assertEquals(model.getBetaValue("X2", Field.P_VALUE), 0.06035232, 0.0000001);
            Assert.assertEquals(model.getBetaValue("X2", Field.CI_LOWER), -0.41356887, 0.0000001);
            Assert.assertEquals(model.getBetaValue("X2", Field.CI_UPPER), 19.051031, 0.0000001);

            Assert.assertEquals(model.getInterceptValue(Field.PARAMETER), -31.76615693, 0.0000001);
            Assert.assertEquals(model.getInterceptValue(Field.STD_ERROR), 74.34447397, 0.0000001);
            Assert.assertEquals(model.getInterceptValue(Field.T_STAT), -0.42728336, 0.0000001);
            Assert.assertEquals(model.getInterceptValue(Field.P_VALUE), 0.67011995, 0.0000001);
            Assert.assertEquals(model.getInterceptValue(Field.CI_LOWER), -179.31935816, 0.0000001);
            Assert.assertEquals(model.getInterceptValue(Field.CI_UPPER), 115.78704429, 0.0000001);

            Assert.assertEquals(model.getN(), 100);
            Assert.assertEquals(model.getRSquared(), 0.9573450120858263, 0.00000001);
            Assert.assertEquals(model.getRSquaredAdj(), 0.9564655277989361, 0.00000001);
            Assert.assertEquals(model.getDurbinWatsonStatistic(), 2.142309721108287, 0.0000001);

            return Optional.empty();
        });

    }


    @Test(dataProvider = "data2")
    public void testMultipleWithoutIntercept(DataFrame<Integer,String> data) {
        final Array<Double> weights = computeWeightsMultiple(data);
        data.regress().wls("Y", Array.of("X1", "X2"), weights, false, model -> {

            System.out.println(model);

            Assert.assertEquals(model.getBetaValue("X1", Field.PARAMETER), 2.997961, 0.0000001);
            Assert.assertEquals(model.getBetaValue("X1", Field.STD_ERROR), 0.33480816, 0.0000001);
            Assert.assertEquals(model.getBetaValue("X1", Field.T_STAT), 8.95426516, 0.0000001);
            Assert.assertEquals(model.getBetaValue("X1", Field.P_VALUE), 0.0000, 0.0000001);
            Assert.assertEquals(model.getBetaValue("X1", Field.CI_LOWER), 2.33345949, 0.0000001);
            Assert.assertEquals(model.getBetaValue("X1", Field.CI_UPPER), 3.66246251, 0.0000001);

            Assert.assertEquals(model.getBetaValue("X2", Field.PARAMETER), 7.22481913, 0.0000001);
            Assert.assertEquals(model.getBetaValue("X2", Field.STD_ERROR), 0.17312297, 0.0000001);
            Assert.assertEquals(model.getBetaValue("X2", Field.T_STAT), 41.73229617, 0.0000001);
            Assert.assertEquals(model.getBetaValue("X2", Field.P_VALUE), 0.0000, 0.0000001);
            Assert.assertEquals(model.getBetaValue("X2", Field.CI_LOWER), 6.88121796, 0.0000001);
            Assert.assertEquals(model.getBetaValue("X2", Field.CI_UPPER), 7.56842031, 0.0000001);

            Assert.assertEquals(model.getN(), 100);
            Assert.assertEquals(model.getRSquared(), 0.9868228339286222, 0.00000001);
            Assert.assertEquals(model.getRSquaredAdj(), 0.9865539121720635, 0.00000001);
            Assert.assertEquals(model.getDurbinWatsonStatistic(), 2.1502412957853356, 0.0000001);

            return Optional.empty();
        });

    }



    /**
     * Returns the vector of weights for the WLS regression by regressing |residuals| on the predictor
     * @param frame     the frame of original data
     * @return          the weight vector for diagonal matrix in WLS
     */
    private Array<Double> computeWeights(DataFrame<Integer,String> frame) {
        return frame.regress().ols("y", "x", true, model -> {
            final DataFrame<Integer,String> residuals = model.getResiduals();
            final DataFrame<Integer,String> residualsAbs = residuals.mapToDoubles(v -> Math.abs(v.getDouble()));
            final DataFrame<Integer,String> xValues = frame.cols().select("x");
            final DataFrame<Integer,String> newData = DataFrame.concatColumns(residualsAbs, xValues);
            return newData.regress().ols("Residuals", "x", false, ols -> {
                final DataFrame<Integer,String> stdDev = ols.getFittedValues();
                final double[] weights = stdDev.colAt(0).toDoubleStream().map(v -> 1d / Math.pow(v, 2d)).toArray();
                return Optional.of(Array.of(weights));
            });
        }).orElse(null);
    }

    /**
     * Returns the vector of weights for the WLS regression by regressing |residuals| on the predictor
     * @param frame     the frame of original data
     * @return          the weight vector for diagonal matrix in WLS
     */
    private Array<Double> computeWeightsMultiple(DataFrame<Integer,String> frame) {
        return frame.regress().ols("Y", Array.of("X1", "X2"), true, model -> {
            final DataFrame<Integer,String> residuals = model.getResiduals();
            final DataFrame<Integer,String> residualsAbs = residuals.mapToDoubles(v -> Math.abs(v.getDouble()));
            final DataFrame<Integer,String> xValues = frame.cols().select("X1");
            final DataFrame<Integer,String> newData = DataFrame.concatColumns(residualsAbs, xValues);
            return newData.regress().ols("Residuals", "X1", false, ols -> {
                final DataFrame<Integer,String> stdDev = ols.getFittedValues();
                final double[] weights = stdDev.colAt(0).toDoubleStream().map(v -> 1d / Math.pow(v, 2d)).toArray();
                return Optional.of(Array.of(weights));
            });
        }).orElse(null);
    }



}
