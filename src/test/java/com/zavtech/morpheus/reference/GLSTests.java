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

import java.time.Year;
import java.util.Optional;

import org.junit.Test;
import org.testng.Assert;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameLeastSquares.Field;
import com.zavtech.morpheus.range.Range;

/**
 * Unit tests for Generalized Least Squares Regression Analysis
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class GLSTests {


    /**
     * Returns the longley dataset
     * @return  longley dataset
     */
    private DataFrame<Year,String> longley() {
        return DataFrame.read().csv(options -> {
            options.setHeader(true);
            options.setExcludeColumnIndexes(0);
            options.setResource("/csv/longley-2.csv");
            options.setRowKeyParser(Year.class, values -> Year.of(Integer.parseInt(values[0])));
        });
    }


    @Test()
    public void testLongleyWithIntercept() {
        final DataFrame<Year,String> frame = longley();
        final String regressand = "TOTEMP";
        final String regressor = "POP";
        final double rho = -0.3634294908770692;
        final DataFrame<Integer,Integer> omega = createOmega(16, rho);

        frame.regress().gls(regressand, regressor, omega, true, model -> {

            System.out.println(model);

            Assert.assertEquals(model.getStdError(), 1218.324139018966, 0.0000001);
            Assert.assertEquals(model.getRSquared(), 0.9769762535757043, 0.0000001);
            Assert.assertEquals(model.getRSquaredAdj(), 0.9753317002596832, 0.0000001);

            Assert.assertEquals(model.getInterceptValue(Field.PARAMETER), 7861.23257489, 0.000001);
            Assert.assertEquals(model.getInterceptValue(Field.STD_ERROR), 3837.68450841, 0.000001);
            Assert.assertEquals(model.getInterceptValue(Field.T_STAT), 2.04843117 , 0.000001);
            Assert.assertEquals(model.getInterceptValue(Field.P_VALUE), 0.05975131, 0.000001);
            Assert.assertEquals(model.getInterceptValue(Field.CI_LOWER), -369.78207118, 0.000001);
            Assert.assertEquals(model.getInterceptValue(Field.CI_UPPER), 16092.24722097, 0.000001);

            Assert.assertEquals(model.getBetaValue("POP", Field.PARAMETER), 0.48947061 , 0.000001);
            Assert.assertEquals(model.getBetaValue("POP", Field.STD_ERROR), 0.03264614, 0.000001);
            Assert.assertEquals(model.getBetaValue("POP", Field.T_STAT), 14.99321343 , 0.000001);
            Assert.assertEquals(model.getBetaValue("POP", Field.P_VALUE), 0.0000, 0.000001);
            Assert.assertEquals(model.getBetaValue("POP", Field.CI_LOWER), 0.41945159, 0.000001);
            Assert.assertEquals(model.getBetaValue("POP", Field.CI_UPPER), 0.55948962, 0.000001);

            return Optional.empty();
        });
    }



    @Test()
    public void testLongleyWithoutIntercept() {
        final DataFrame<Year,String> frame = longley();
        final String regressand = "TOTEMP";
        final String regressor = "POP";
        final double rho = -0.3634294908770692;
        final DataFrame<Integer,Integer> omega = createOmega(16, rho);

        frame.regress().gls(regressand, regressor, omega, false, model -> {

            System.out.println(model);

            Assert.assertEquals(model.getStdError(), 1341.8563391142147, 0.0000001);
            Assert.assertEquals(model.getRSquared(), 0.9998093535335607, 0.0000001);
            Assert.assertEquals(model.getRSquaredAdj(), 0.9997966437691314, 0.0000001);

            Assert.assertEquals(model.getBetaValue("POP", Field.PARAMETER), 0.55624219 , 0.000001);
            Assert.assertEquals(model.getBetaValue("POP", Field.STD_ERROR), 0.00198323, 0.000001);
            Assert.assertEquals(model.getBetaValue("POP", Field.T_STAT), 280.47221682 , 0.000001);
            Assert.assertEquals(model.getBetaValue("POP", Field.P_VALUE), 0.0000, 0.000001);
            Assert.assertEquals(model.getBetaValue("POP", Field.CI_LOWER), 0.55198857, 0.000001);
            Assert.assertEquals(model.getBetaValue("POP", Field.CI_UPPER), 0.5604958, 0.000001);

            return Optional.empty();
        });
    }




    /**
     * Test multiple GLS kitchen sink regression with intercept
     */
    @Test()
    public void testLongleyMultipleWithIntercept() {
        final String regressand = "TOTEMP";
        final double rho = -0.3634294908770692;
        final Array<String> regressors = Array.of("GNPDEFL", "GNP", "UNEMP", "ARMED", "POP", "YEAR");
        final DataFrame<Year,String> frame = longley();
        final DataFrame<Integer,Integer> omega = createOmega(16, rho);

        frame.regress().gls(regressand, regressors, omega, true, model -> {

            System.out.println(model);

            Assert.assertEquals(model.getInterceptValue(Field.PARAMETER), -3797854.9015416, 0.001);
            Assert.assertEquals(model.getInterceptValue(Field.STD_ERROR), 670688.69930821, 0.0000001);
            Assert.assertEquals(model.getInterceptValue(Field.T_STAT), -5.66261949, 0.0000001);
            Assert.assertEquals(model.getInterceptValue(Field.P_VALUE), 0.00030861, 0.0000001);
            Assert.assertEquals(model.getInterceptValue(Field.CI_LOWER), -5315058.1466895, 0.0000001);
            Assert.assertEquals(model.getInterceptValue(Field.CI_UPPER), -2280651.6563937, 0.0000001);

            Assert.assertEquals(model.getBetaValue("GNPDEFL", Field.PARAMETER), -12.76564544, 0.0000001);
            Assert.assertEquals(model.getBetaValue("GNPDEFL", Field.STD_ERROR), 69.43080733, 0.0000001);
            Assert.assertEquals(model.getBetaValue("GNPDEFL", Field.T_STAT), -0.1838614, 0.0000001);
            Assert.assertEquals(model.getBetaValue("GNPDEFL", Field.P_VALUE), 0.85819798, 0.0000001);
            Assert.assertEquals(model.getBetaValue("GNPDEFL", Field.CI_LOWER), -169.82904357, 0.0000001);
            Assert.assertEquals(model.getBetaValue("GNPDEFL", Field.CI_UPPER), 144.29775269, 0.0000001);

            Assert.assertEquals(model.getBetaValue("GNP", Field.PARAMETER), -0.03800132, 0.0000001);
            Assert.assertEquals(model.getBetaValue("GNP", Field.STD_ERROR), 0.02624768, 0.0000001);
            Assert.assertEquals(model.getBetaValue("GNP", Field.T_STAT), -1.44779736, 0.0000001);
            Assert.assertEquals(model.getBetaValue("GNP", Field.P_VALUE), 0.1815954, 0.0000001);
            Assert.assertEquals(model.getBetaValue("GNP", Field.CI_LOWER), -0.09737771, 0.0000001);
            Assert.assertEquals(model.getBetaValue("GNP", Field.CI_UPPER), 0.02137506, 0.0000001);

            Assert.assertEquals(model.getBetaValue("UNEMP", Field.PARAMETER), -2.18694871, 0.0000001);
            Assert.assertEquals(model.getBetaValue("UNEMP", Field.STD_ERROR), 0.38239315, 0.0000001);
            Assert.assertEquals(model.getBetaValue("UNEMP", Field.T_STAT), -5.71911057, 0.0000001);
            Assert.assertEquals(model.getBetaValue("UNEMP", Field.P_VALUE), 0.00028728, 0.0000001);
            Assert.assertEquals(model.getBetaValue("UNEMP", Field.CI_LOWER), -3.05198212, 0.0000001);
            Assert.assertEquals(model.getBetaValue("UNEMP", Field.CI_UPPER), -1.32191531, 0.0000001);

            Assert.assertEquals(model.getBetaValue("ARMED", Field.PARAMETER), -1.15177649, 0.0000001);
            Assert.assertEquals(model.getBetaValue("ARMED", Field.STD_ERROR), 0.16525269, 0.0000001);
            Assert.assertEquals(model.getBetaValue("ARMED", Field.T_STAT), -6.96978961, 0.0000001);
            Assert.assertEquals(model.getBetaValue("ARMED", Field.P_VALUE), 0.0000654, 0.0000001);
            Assert.assertEquals(model.getBetaValue("ARMED", Field.CI_LOWER), -1.52560405, 0.0000001);
            Assert.assertEquals(model.getBetaValue("ARMED", Field.CI_UPPER), -0.77794893, 0.0000001);

            Assert.assertEquals(model.getBetaValue("POP", Field.PARAMETER), -0.06805356, 0.0000001);
            Assert.assertEquals(model.getBetaValue("POP", Field.STD_ERROR), 0.17642833, 0.0000001);
            Assert.assertEquals(model.getBetaValue("POP", Field.T_STAT), -0.38572919, 0.0000001);
            Assert.assertEquals(model.getBetaValue("POP", Field.P_VALUE), 0.70865648, 0.0000001);
            Assert.assertEquals(model.getBetaValue("POP", Field.CI_LOWER), -0.46716218, 0.0000001);
            Assert.assertEquals(model.getBetaValue("POP", Field.CI_UPPER), 0.33105506, 0.0000001);

            Assert.assertEquals(model.getBetaValue("YEAR", Field.PARAMETER), 1993.95292851, 0.0000001);
            Assert.assertEquals(model.getBetaValue("YEAR", Field.STD_ERROR), 342.63462757, 0.0000001);
            Assert.assertEquals(model.getBetaValue("YEAR", Field.T_STAT), 5.8194729, 0.0000001);
            Assert.assertEquals(model.getBetaValue("YEAR", Field.P_VALUE), 0.00025323, 0.0000001);
            Assert.assertEquals(model.getBetaValue("YEAR", Field.CI_LOWER), 1218.85955153, 0.0000001);
            Assert.assertEquals(model.getBetaValue("YEAR", Field.CI_UPPER), 2769.04630548, 0.0000001);

            Assert.assertEquals(model.getN(), 16);
            Assert.assertEquals(model.getRSquared(), 0.9991880200816362, 0.00000001);
            Assert.assertEquals(model.getRSquaredAdj(), 0.9986467001360635, 0.00000001);
            Assert.assertEquals(model.getDurbinWatsonStatistic(), 2.677871902537079, 0.0000001);

            return Optional.empty();
        });
    }


    /**
     * Test multiple GLS kitchen sink regression without intercept
     */
    @Test()
    public void testLongleyMultipleWithoutIntercept() {
        final double rho = -0.3634294908770692;
        final DataFrame<Year,String> frame = longley();
        final DataFrame<Integer,Integer> omega = createOmega(16, rho);
        final String regressand = "TOTEMP";
        final Array<String> regressors = Array.of("GNPDEFL", "GNP", "UNEMP", "ARMED", "POP", "YEAR");

        frame.regress().gls(regressand, regressors, omega, false, model -> {

            System.out.println(model);

            Assert.assertEquals(model.getBetaValue("GNPDEFL", Field.PARAMETER), -95.3398763, 0.0000001);
            Assert.assertEquals(model.getBetaValue("GNPDEFL", Field.STD_ERROR), 137.56031582, 0.0000001);
            Assert.assertEquals(model.getBetaValue("GNPDEFL", Field.T_STAT), -0.69307689, 0.0000001);
            Assert.assertEquals(model.getBetaValue("GNPDEFL", Field.P_VALUE), 0.50575132, 0.0000001);
            Assert.assertEquals(model.getBetaValue("GNPDEFL", Field.CI_LOWER), -406.52293004, 0.0000001);
            Assert.assertEquals(model.getBetaValue("GNPDEFL", Field.CI_UPPER), 215.84317744, 0.0000001);

            Assert.assertEquals(model.getBetaValue("GNP", Field.PARAMETER), 0.08258988, 0.0000001);
            Assert.assertEquals(model.getBetaValue("GNP", Field.STD_ERROR), 0.03109279, 0.0000001);
            Assert.assertEquals(model.getBetaValue("GNP", Field.T_STAT), 2.6562392, 0.0000001);
            Assert.assertEquals(model.getBetaValue("GNP", Field.P_VALUE), 0.02620683, 0.0000001);
            Assert.assertEquals(model.getBetaValue("GNP", Field.CI_LOWER), 0.01225311, 0.0000001);
            Assert.assertEquals(model.getBetaValue("GNP", Field.CI_UPPER), 0.15292665, 0.0000001);

            Assert.assertEquals(model.getBetaValue("UNEMP", Field.PARAMETER), -0.38464658, 0.0000001);
            Assert.assertEquals(model.getBetaValue("UNEMP", Field.STD_ERROR), 0.42950342, 0.0000001);
            Assert.assertEquals(model.getBetaValue("UNEMP", Field.T_STAT), -0.89556116, 0.0000001);
            Assert.assertEquals(model.getBetaValue("UNEMP", Field.P_VALUE), 0.39380879, 0.0000001);
            Assert.assertEquals(model.getBetaValue("UNEMP", Field.CI_LOWER), -1.35625083, 0.0000001);
            Assert.assertEquals(model.getBetaValue("UNEMP", Field.CI_UPPER), 0.58695766, 0.0000001);

            Assert.assertEquals(model.getBetaValue("ARMED", Field.PARAMETER), -0.62180232, 0.0000001);
            Assert.assertEquals(model.getBetaValue("ARMED", Field.STD_ERROR), 0.27599301, 0.0000001);
            Assert.assertEquals(model.getBetaValue("ARMED", Field.T_STAT), -2.25296405, 0.0000001);
            Assert.assertEquals(model.getBetaValue("ARMED", Field.P_VALUE), 0.05075685, 0.0000001);
            Assert.assertEquals(model.getBetaValue("ARMED", Field.CI_LOWER), -1.24614187, 0.0000001);
            Assert.assertEquals(model.getBetaValue("ARMED", Field.CI_UPPER), 0.00253724, 0.0000001);

            Assert.assertEquals(model.getBetaValue("POP", Field.PARAMETER), -0.51485476, 0.0000001);
            Assert.assertEquals(model.getBetaValue("POP", Field.STD_ERROR), 0.31977699, 0.0000001);
            Assert.assertEquals(model.getBetaValue("POP", Field.T_STAT), -1.61004317, 0.0000001);
            Assert.assertEquals(model.getBetaValue("POP", Field.P_VALUE), 0.1418488, 0.0000001);
            Assert.assertEquals(model.getBetaValue("POP", Field.CI_LOWER), -1.23824056, 0.0000001);
            Assert.assertEquals(model.getBetaValue("POP", Field.CI_UPPER), 0.20853105, 0.0000001);

            Assert.assertEquals(model.getBetaValue("YEAR", Field.PARAMETER), 54.38804549, 0.0000001);
            Assert.assertEquals(model.getBetaValue("YEAR", Field.STD_ERROR), 17.89702909, 0.0000001);
            Assert.assertEquals(model.getBetaValue("YEAR", Field.T_STAT), 3.03894268, 0.0000001);
            Assert.assertEquals(model.getBetaValue("YEAR", Field.P_VALUE), 0.01404192, 0.0000001);
            Assert.assertEquals(model.getBetaValue("YEAR", Field.CI_LOWER), 13.90215294, 0.0000001);
            Assert.assertEquals(model.getBetaValue("YEAR", Field.CI_UPPER), 94.87393803, 0.0000001);

            Assert.assertEquals(model.getN(), 16);
            Assert.assertEquals(model.getRSquared(), 0.9999763962749031, 0.00000001);
            Assert.assertEquals(model.getRSquaredAdj(), 0.999962234039845, 0.00000001);
            Assert.assertEquals(model.getDurbinWatsonStatistic(), 0.9958792710534731, 0.0000001);

            return Optional.empty();
        });
    }





    /**
     * Returns the correlation matrix omega
     * @param size      the size for the correlation matrix
     * @param autocorr  the auto correlation value to base matrix on
     * @return          the newly created correlation matrix
     */
    private DataFrame<Integer,Integer> createOmega(int size, double autocorr) {
        final Range<Integer> keys = Range.of(0, size);
        return DataFrame.ofDoubles(keys, keys, v -> {
            return Math.pow(autocorr,  Math.abs(v.rowOrdinal() - v.colOrdinal()));
        });
    }
}
