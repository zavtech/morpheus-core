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

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.zavtech.morpheus.util.Asserts.assertEquals;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameLeastSquares;
import com.zavtech.morpheus.frame.DataFrameLeastSquares.Field;
import com.zavtech.morpheus.index.Index;
import com.zavtech.morpheus.range.Range;

/**
 * Unit tests for Ordinary Least Squares Regression Analysis
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class OLSTests {

    @DataProvider(name="thresholds")
    public Object[][] thresholds() {
        return new Object[][] { {0d}, {0.1d}};
    }


    @DataProvider(name="solver")
    public Object[][] solver() {
        return new Object[][] {
            { DataFrameLeastSquares.Solver.INV },
            { DataFrameLeastSquares.Solver.QR }
        };
    }

    /**
     * Returns a DataFrame of motor vehicles features
     * @return  the frame of motor vehicle features
     */
    private static DataFrame<Integer,String> loadCarDataset() {
        return DataFrame.read().csv(options -> {
            options.setResource("/csv/cars93.csv");
            options.setExcludeColumnIndexes(0);
        });
    }



    @Test(dataProvider = "solver")
    public void ols2(DataFrameLeastSquares.Solver solver) throws Exception {
        DataFrame<Integer,String> frame = loadCarDataset();
        final String regressand = "Horsepower";
        final String regressor = "EngineSize";
        frame.regress().ols(regressand, regressor, true, model -> {
            model.withSolver(solver);
            assert (model.getRegressand().equals(regressand));
            assert (model.getRegressors().size() == 1);
            assertEquals(model.getRSquared(), 0.5359992996664269, 0.00001);
            assertEquals(model.getRSquaredAdj(), 0.5309003908715525, 0.000001);
            assertEquals(model.getStdError(), 35.87167658782274, 0.00001);
            assertEquals(model.getFValue(), 105.120393642, 0.00001);
            assertEquals(model.getFValueProbability(), 0, 0.00001);
            assertEquals(model.getBetaValue("EngineSize", Field.PARAMETER), 36.96327914, 0.0000001);
            assertEquals(model.getBetaValue("EngineSize", Field.STD_ERROR), 3.60518041, 0.0000001);
            assertEquals(model.getBetaValue("EngineSize", Field.T_STAT), 10.25282369, 0.0000001);
            assertEquals(model.getBetaValue("EngineSize", Field.P_VALUE), 0.0000, 0.0000001);
            assertEquals(model.getBetaValue("EngineSize", Field.CI_LOWER), 29.80203113, 0.0000001);
            assertEquals(model.getBetaValue("EngineSize", Field.CI_UPPER), 44.12452714, 0.0000001);
            assertEquals(model.getInterceptValue(Field.PARAMETER), 45.21946716, 0.0000001);
            assertEquals(model.getInterceptValue(Field.STD_ERROR), 10.31194906, 0.0000001);
            assertEquals(model.getInterceptValue(Field.T_STAT), 4.3851523, 0.0000001);
            assertEquals(model.getInterceptValue(Field.P_VALUE), 0.00003107, 0.0000001);
            assertEquals(model.getInterceptValue(Field.CI_LOWER), 24.73604714, 0.0000001);
            assertEquals(model.getInterceptValue(Field.CI_UPPER), 65.70288719, 0.0000001);
            System.out.println(model);
            return Optional.of(model);
        });
    }


    @Test(dataProvider = "solver")
    public void ols3(DataFrameLeastSquares.Solver solver) throws Exception {
        DataFrame<Integer,String> frame = loadCarDataset();
        final String regressand = "Horsepower";
        final String regressor = "EngineSize";
        frame.regress().ols(regressand, regressor, false, model -> {
            model.withSolver(solver);
            assert (model.getRegressand().equals(regressand));
            assert (model.getRegressors().size() == 1);
            assertEquals(model.getRSquared(), 0.934821940829, 0.00001);
            assertEquals(model.getRSquaredAdj(), 0.9341134836, 0.000001);
            assertEquals(model.getStdError(), 39.26510834, 0.00001);
            assertEquals(model.getFValue(), 1319.517942852, 0.00001);
            assertEquals(model.getFValueProbability(), 0, 0.00001);
            assertEquals(model.getBetaValue("EngineSize", Field.PARAMETER), 51.708176166, 0.0000001);
            assertEquals(model.getBetaValue("EngineSize", Field.STD_ERROR), 1.4234806556, 0.0000001);
            assertEquals(model.getBetaValue("EngineSize", Field.T_STAT), 36.32516955, 0.0000001);
            assertEquals(model.getBetaValue("EngineSize", Field.P_VALUE), 5.957E-56, 0.0000001);
            assertEquals(model.getBetaValue("EngineSize", Field.CI_LOWER), 48.880606712, 0.0000001);
            assertEquals(model.getBetaValue("EngineSize", Field.CI_UPPER), 54.53574562, 0.0000001);
            System.out.println(model);
            return Optional.of(model);
        });
    }




    @Test()
    public void testSLR() {
        final Index<String> rowKeys = Range.of(0, 5000).map(i -> "R" + i).toIndex(String.class);
        final DataFrame<String,String> frame = DataFrame.of(rowKeys, String.class, columns -> {
            columns.add("Y", Double.class).applyDoubles(v -> v.rowOrdinal() + 1d + (Math.random() * 2));
            columns.add("X", Double.class).applyDoubles(v -> v.rowOrdinal() + 1d + (Math.random() * 3));
        });
        final SimpleRegression benchmark = new SimpleRegression(true);
        frame.rows().forEach(row -> benchmark.addData(row.getDouble(1), row.getDouble(0)));
        frame.regress().ols("Y", "X", true, model -> {
            assertResultsMatch(frame, model, benchmark);
            return Optional.empty();
        });
    }


    @Test(dataProvider = "thresholds")
    public void testMLR(double threshold) {
        final Range<String> rowKeys = Range.of(0, 5000).map(i -> "R" + i);
        final DataFrame<String,String> frame = DataFrame.of(rowKeys, String.class, columns -> {
            columns.add("Y", Double.class).applyDoubles(v -> v.rowOrdinal() + 1d + (Math.random() * 2));
            columns.add("X1", Double.class).applyDoubles(v -> v.rowOrdinal() + 1d + (Math.random() * 3));
            columns.add("X2", Double.class).applyDoubles(v -> v.rowOrdinal() + 1d + (Math.random() * 4));
            columns.add("X3", Double.class).applyDoubles(v -> v.rowOrdinal() + 1d + (Math.random() * 5));
            columns.add("X4", Double.class).applyDoubles(v -> v.rowOrdinal() + 1d + (Math.random() * 6));
        });

        final OLSMultipleLinearRegression model = new OLSMultipleLinearRegression(threshold);
        final double[] dependent = frame.colAt("Y").toDoubleStream().toArray();
        final double[][] independent = new double[frame.rows().count()][3];
        frame.rows().forEach(row -> {
            independent[row.ordinal()][0] = row.getDouble("X1");
            independent[row.ordinal()][1] = row.getDouble("X2");
            independent[row.ordinal()][2] = row.getDouble("X3");
        });
        model.newSampleData(dependent, independent);
        frame.regress().ols("Y", Arrays.asList("X1", "X2", "X3"), true, mlr -> {
            System.out.println(mlr);
            assertResultsMatch(mlr, model);
            return Optional.empty();
        });
    }

    @Test(description = "Tests whether a Simple regression results matches a Multiple regression result with one parameter")
    public void testSLRvsMLR() {
        final Range<String> rowKeys = Range.of(0, 5000).map(i -> "R" + i);
        final DataFrame<String,String> frame = DataFrame.of(rowKeys, String.class, columns -> {
            columns.add("Y", Double.class).applyDoubles(v -> v.rowOrdinal() + 1d + (Math.random() * 2));
            columns.add("X", Double.class).applyDoubles(v -> v.rowOrdinal() + 1d + (Math.random() * 3));
        });

        frame.regress().ols("Y", "X", true, slr -> {
            frame.regress().ols("Y", Collections.singletonList("X"), true, mlr -> {
                Assert.assertEquals(slr.getResidualSumOfSquares(), mlr.getResidualSumOfSquares(), 0.0000001, "Residual sum of squares matches");
                Assert.assertEquals(slr.getTotalSumOfSquares(), mlr.getTotalSumOfSquares(), 0.1, "Total sum of squares matches");
                Assert.assertEquals(slr.getRSquared(), mlr.getRSquared(), 0.0000001, "R^2 values match");
                Assert.assertEquals(slr.getStdError(), mlr.getStdError(),  0.0000001, "Std error matches");
                Assert.assertEquals(slr.getBetaValue("X", Field.PARAMETER), mlr.getBetaValue("X", Field.PARAMETER), 0.00000001, "Beta parameters match");
                Assert.assertEquals(slr.getBetaValue("X", Field.STD_ERROR), mlr.getBetaValue("X", Field.STD_ERROR), 0.000000001, "Beta standard errors match");
                Assert.assertEquals(slr.getInterceptValue(Field.STD_ERROR), mlr.getInterceptValue(Field.STD_ERROR), 0.00000001, "Intercept standard errors match");

                final DataFrame<String,String> residuals1 = slr.getResiduals();
                final DataFrame<String,String> residuals2 = mlr.getResiduals();

                Assert.assertEquals(residuals1.rows().count(), residuals2.rows().count(), "Same number of residuals");
                residuals1.rows().forEach(row -> {
                    final double v1 = row.getDouble(0);
                    final double v2 = residuals2.data().getDouble(row.ordinal(), 0);
                    Assert.assertEquals(v1, v2, 0.0000001, "Residuals match for index " + row.ordinal());
                });
                return Optional.empty();
            });
            return Optional.empty();
        });
    }

    /**
     * Checks that the Morpheus OLS model yields the same results as Apache Math
     * @param actual    the Morpheus results
     * @param expected  the Apache results
     */
    private <R,C> void assertResultsMatch(DataFrameLeastSquares<R,C> actual, OLSMultipleLinearRegression expected) {

        Assert.assertEquals(actual.getResidualSumOfSquares(), expected.calculateResidualSumOfSquares(), 0.0000001, "Residual sum of squares matches");
        Assert.assertEquals(actual.getTotalSumOfSquares(), expected.calculateTotalSumOfSquares(), actual.getTotalSumOfSquares() * 0.000001, "Total sum of squares matches");
        Assert.assertEquals(actual.getRSquared(), expected.calculateRSquared(), 0.0000001, "R^2 values match");
        Assert.assertEquals(actual.getStdError(), expected.estimateRegressionStandardError(),  0.0000001, "Std error matches");

        final DataFrame<C,Field> params1 = actual.getBetas();
        final double[] params2 = expected.estimateRegressionParameters();
        Assert.assertEquals(params1.rows().count(), params2.length-1, "Same number of parameters");
        for (int i=0; i<params1.rows().count(); ++i) {
            final double actualParam = params1.data().getDouble(i, Field.PARAMETER);
            final double expectedParam = params2[i+1];
            Assert.assertEquals(actualParam, expectedParam, 0.000000001, "Parameters match at index " + i);
        }

        final double intercept = expected.estimateRegressionParameters()[0];
        final double interceptStdError = expected.estimateRegressionParametersStandardErrors()[0];
        Assert.assertEquals(actual.getInterceptValue(Field.PARAMETER), intercept,  0.0000001, "The intercepts match");
        Assert.assertEquals(actual.getInterceptValue(Field.STD_ERROR), interceptStdError, 0.000000001, "The intercept std errors match");

        final DataFrame<R,String> residuals1 = actual.getResiduals();
        final double[] residuals2 = expected.estimateResiduals();
        Assert.assertEquals(residuals1.rows().count(), residuals2.length, "Same number of residuals");
        for (int i=0; i<residuals1.rows().count(); ++i) {
            Assert.assertEquals(residuals1.data().getDouble(i, 0), residuals2[i], 0.00000001, "Residuals match at index " + i);
        }

        final DataFrame<C,Field> stdErrs1 = actual.getBetas().cols().select(c -> c.key() == Field.STD_ERROR);
        final double[] stdErrs2 = expected.estimateRegressionParametersStandardErrors();
        Assert.assertEquals(stdErrs1.rows().count(), stdErrs2.length-1, "Same number of parameter standard errors");
        for (int i=0; i<stdErrs1.cols().count(); ++i) {
            Assert.assertEquals(stdErrs1.data().getDouble(0, i), stdErrs2[i+1], 0.00000001, "Standard errors match at index " + i);
        }
    }


    /**
     * Checks that the Morpheus OLS model yields the same results as Apache Math
     * @param frame      the data for regression
     * @param actual    the Morpheus results
     * @param expected  the Apache results
     */
    private <R> void assertResultsMatch(DataFrame<String,String> frame, DataFrameLeastSquares<String,String> actual, SimpleRegression expected) {

        final double tss1 = actual.getTotalSumOfSquares();
        final double tss2 = expected.getTotalSumSquares();
        final double threshold = ((tss1 + tss2) / 2d) * 0.00001d;

        Assert.assertEquals(actual.getTotalSumOfSquares(), expected.getTotalSumSquares(), threshold, "Total sum of squares matches");
        Assert.assertEquals(actual.getRSquared(), expected.getRSquare(), 0.0000001, "R^2 values match");

        final double beta1 = actual.getBetaValue("X", Field.PARAMETER);
        final double beta2 = expected.getSlope();
        Assert.assertEquals(beta1, beta2, 0.000001, "Beta parameters match");

        final double intercept = expected.getIntercept();
        final double interceptStdError = expected.getInterceptStdErr();
        Assert.assertEquals(actual.getInterceptValue(Field.PARAMETER), intercept,  0.0000001, "The intercepts match");
        Assert.assertEquals(actual.getInterceptValue(Field.STD_ERROR), interceptStdError, 0.000000001, "The intercept std errors match");

        final double betaStdErr1 = actual.getBetaValue("X", Field.STD_ERROR);
        final double betaStdErr2 = expected.getSlopeStdErr();
        Assert.assertEquals(betaStdErr1, betaStdErr2, 0.00000001, "Beta Standard errors match");

        final DataFrame<String,String> residuals = actual.getResiduals();
        Assert.assertEquals(residuals.rows().count(), frame.rows().count(), "There are expected number of residuals");
        residuals.rows().forEach(row -> {
            final double x = frame.data().getDouble(row.ordinal(), "X");
            final double y = frame.data().getDouble(row.ordinal(), "Y");
            final double residual = row.getDouble(0);
            final double expect = y - expected.predict(x);
            Assert.assertEquals(residual, expect, 0.0000001, "Residual matches for x=" + x + " at row index " + row.ordinal());
        });
    }

}
