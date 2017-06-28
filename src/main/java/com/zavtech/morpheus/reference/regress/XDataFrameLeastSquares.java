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
package com.zavtech.morpheus.reference.regress;

import java.io.ByteArrayOutputStream;
import java.text.DecimalFormat;
import java.text.Format;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

import org.apache.commons.math3.distribution.FDistribution;
import org.apache.commons.math3.distribution.TDistribution;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.LUDecomposition;
import org.apache.commons.math3.linear.QRDecomposition;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealVector;

import com.zavtech.morpheus.array.Array;
import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFrameLeastSquares;
import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.util.text.printer.Printer;

/**
 * An abstract base class for creating linear regression models based on Least Squares estimators such as OLS and GLS.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
abstract class XDataFrameLeastSquares<R,C> implements DataFrameLeastSquares<R,C> {

    private static final List<Field> fields = Arrays.asList(Field.values());

    private String name;
    private double alpha;
    private C regressand;
    private List<C> regressors;
    private Solver solver;
    private DataFrame<R,C> frame;
    private double tss;
    private double rss;
    private double ess;
    private double stdError;
    private double threshold;
    private double rSquared;
    private double rSquaredAdj;
    private double errorVariance;
    private boolean hasIntercept;
    private double fValue;
    private double fValueProbability;
    private long runtimeMillis;
    private DataFrame<C,Field> betas;
    private DataFrame<R,String> residuals;
    private DataFrame<String,Field> intercept;


    /**
     * Constructor
     * @param name          the regression model name (OLS, WLS, GLS)
     * @param frame         the frame to operate on
     * @param regressand    the regressand key that defines the dependent variable
     * @param regressors    the regressor keys that define the independent variables
     * @param intercept     true to include an intercept / constant term in the model
     */
    XDataFrameLeastSquares(String name, DataFrame<R,C> frame, C regressand, List<C> regressors, boolean intercept) {
        if (regressors.size() == 0) {
            throw new DataFrameException("At least one regressor must be specified");
        } else {
            this.name = name;
            this.alpha = 0.05;
            this.frame = frame;
            this.threshold = 0d;
            this.solver = Solver.QR;
            this.regressand = regressand;
            this.hasIntercept = intercept;
            this.regressors = new ArrayList<>(regressors);
            this.betas = DataFrame.ofDoubles(regressors, fields);
            this.intercept = DataFrame.ofDoubles("Intercept", fields);
        }
    }

    /**
     * Triggers a full (re)compute of this regression model
     * @throws DataFrameException   if model fit fails
     */
    abstract void compute();


    @Override
    public void fit() {
        try {
            final long t1 = System.nanoTime();
            this.compute();
            final long t2 = System.nanoTime();
            this.runtimeMillis = (t2-t1)/1000000;
        } catch (DataFrameException ex) {
            throw ex;
        } catch (Exception ex) {
            final String regressand = getRegressand().toString();
            final String regressors = Arrays.toString(getRegressors().toArray());
            throw new DataFrameException("Failed white running linear regression of " + regressand + " on " + regressors, ex);
        }
    }


    /**
     * Returns a reference to the frame for this model
     * @return      the frame for this model
     */
    protected DataFrame<R,C> frame() {
        return frame;
    }


    /**
     * Returns true if model setup has changed and needs to be re-calculated
     * @return  true if model is dirty and needs to be re-calculated
     */
    private boolean isDirty() {
        return residuals == null;
    }


    /**
     * Computes the model if necessary
     */
    private void computeIf() {
        if (isDirty()) {
            fit();
        }
    }


    /**
     * Creates the Y vector for this regression model
     * @return  the Y vector for regression model
     */
    RealVector createY() {
        final int rowCount = frame.rows().count();
        final int colIndex = frame.cols().ordinalOf(regressand);
        final RealVector y = new ArrayRealVector(rowCount);
        for (int i = 0; i < rowCount; ++i) {
            y.setEntry(i, frame.data().getDouble(i, colIndex));
        }
        return y;
    }


    /**
     * Creates the X design matrix for this regression model
     * @return  the X design matrix
     */
    RealMatrix createX() {
        final int n = frame.rows().count();
        final int offset = hasIntercept() ? 1 : 0;
        final int p = hasIntercept() ? regressors.size() + 1 : regressors.size();
        final int[] colIndexes = regressors.stream().mapToInt(k -> frame.cols().ordinalOf(k)).toArray();
        final RealMatrix x = new Array2DRowRealMatrix(n, p);
        for (int i = 0; i < n; ++i) {
            x.setEntry(i, 0, 1d);
            for (int j = offset; j < p; ++j) {
                final double value = frame.data().getDouble(i, colIndexes[j - offset]);
                x.setEntry(i, j, value);
            }
        }
        return x;
    }


    /**
     * Runs the regression model for the given dependent and independent variables
     * The Y and X variables must be transformed, if necessary, to meet Gauss Markov assumptions
     * @param y     the dependent variable, which may be a transformed version of the raw data
     * @param x     the independent variable(s), which may be a transformed version of the raw data
     */
    protected void compute(RealVector y, RealMatrix x) {
        final int n = frame.rows().count();
        final int p = regressors.size() + (hasIntercept() ? 1 : 0);
        final int dfModel = regressors.size();
        final RealMatrix betaMatrix = computeBeta(y, x);
        final RealVector betaCoefficients = betaMatrix.getColumnVector(0);
        final RealVector betaVariance = betaMatrix.getColumnVector(1);
        this.tss = computeTSS(y);
        this.ess = tss - rss;
        this.fValue = (ess / dfModel) / (rss / (n - p));
        this.fValueProbability = 1d - new FDistribution(dfModel, n-p).cumulativeProbability(fValue);
        this.rSquared = 1d - (rss / tss);
        this.rSquaredAdj = 1d - (rss * (n - (hasIntercept() ? 1 : 0))) / (tss * (n - p));
        this.computeParameterStdErrors(betaVariance);
        this.computeParameterSignificance(betaCoefficients);
    }


    /**
     *
     * @param y     the response vector
     * @param x     the design matrix
     */
    private RealMatrix computeBeta(RealVector y, RealMatrix x) {
        if (solver == Solver.QR) {
            return computeBetaQR(y, x);
        } else {
            final int n = x.getRowDimension();
            final int p = x.getColumnDimension();
            final int offset = hasIntercept() ? 1 : 0;
            final RealMatrix xT = x.transpose();
            final RealMatrix xTxInv = new LUDecomposition(xT.multiply(x)).getSolver().getInverse();
            final RealVector betaVector = xTxInv.multiply(xT).operate(y);
            final RealVector residuals = y.subtract(x.operate(betaVector));
            this.rss = residuals.dotProduct(residuals);
            this.errorVariance = rss / (n - p);
            this.stdError = Math.sqrt(errorVariance);
            this.residuals = createResidualsFrame(residuals);
            final RealMatrix covMatrix = xTxInv.scalarMultiply(errorVariance);
            final RealMatrix result = new Array2DRowRealMatrix(p, 2);
            if (hasIntercept()) {
                result.setEntry(0, 0, betaVector.getEntry(0));      //Intercept coefficient
                result.setEntry(0, 1, covMatrix.getEntry(0, 0));    //Intercept variance
            }
            for (int i = 0; i < getRegressors().size(); i++) {
                final int index = i + offset;
                final double variance = covMatrix.getEntry(index, index);
                result.setEntry(index, 1, variance);
                result.setEntry(index, 0, betaVector.getEntry(index));
            }
            return result;
        }
    }


    /**
     * Computes model parameters and parameter variance using a QR decomposition of the X matrix
     * @param y     the response vector
     * @param x     the design matrix
     */
    private RealMatrix computeBetaQR(RealVector y, RealMatrix x) {
        final int n = x.getRowDimension();
        final int p = x.getColumnDimension();
        final int offset = hasIntercept() ? 1 : 0;
        final QRDecomposition decomposition = new QRDecomposition(x, threshold);
        final RealVector betaVector = decomposition.getSolver().solve(y);
        final RealVector residuals = y.subtract(x.operate(betaVector));
        this.rss = residuals.dotProduct(residuals);
        this.errorVariance = rss / (n - p);
        this.stdError = Math.sqrt(errorVariance);
        this.residuals = createResidualsFrame(residuals);
        final RealMatrix rAug = decomposition.getR().getSubMatrix(0, p - 1, 0, p - 1);
        final RealMatrix rInv = new LUDecomposition(rAug).getSolver().getInverse();
        final RealMatrix covMatrix = rInv.multiply(rInv.transpose()).scalarMultiply(errorVariance);
        final RealMatrix result = new Array2DRowRealMatrix(p, 2);
        if (hasIntercept()) {
            result.setEntry(0, 0, betaVector.getEntry(0));      //Intercept coefficient
            result.setEntry(0, 1, covMatrix.getEntry(0, 0));    //Intercept variance
        }
        for (int i = 0; i < getRegressors().size(); i++) {
            final int index = i + offset;
            final double variance = covMatrix.getEntry(index, index);
            result.setEntry(index, 1, variance);
            result.setEntry(index, 0, betaVector.getEntry(index));
        }
        return result;
    }


    /**
     * Returns a newly created frame of regression residuals
     * @param residuals     the Apache math vector of residuals
     * @return              the Morpheus DataFrame of residuals
     */
    private DataFrame<R,String> createResidualsFrame(RealVector residuals) {
        final Array<R> keys = frame.rows().keyArray();
        final DataFrame<R,String> result = DataFrame.ofDoubles(keys, "Residuals");
        result.applyDoubles(v -> residuals.getEntry(v.rowOrdinal()));
        return result;
    }


    /**
     * Calculates the standard errors of the regression parameters.
     * @param betaVar   the variance of the beta parameters
     * @throws DataFrameException   if this operation fails
     */
    private void computeParameterStdErrors(RealVector betaVar) {
        try {
            final int offset = hasIntercept() ? 1 : 0;
            if (hasIntercept()) {
                final double interceptVariance = betaVar.getEntry(0);
                final double interceptStdError = Math.sqrt(interceptVariance);
                this.intercept.data().setDouble(0, Field.STD_ERROR, interceptStdError);
            }
            for (int i = 0; i < regressors.size(); i++) {
                final double betaVar_i = betaVar.getEntry(i + offset);
                final double betaStdError = Math.sqrt(betaVar_i);
                this.betas.data().setDouble(i, Field.STD_ERROR, betaStdError);
            }
        } catch (Exception ex) {
            throw new DataFrameException("Failed to calculate regression coefficient standard errors", ex);
        }
    }


    /**
     * Computes the T-stats and the P-Value for all regression parameters
     */
    private void computeParameterSignificance(RealVector betaVector) {
        try {
            final double residualDF = frame.rows().count() - (regressors.size() + 1);
            final TDistribution distribution = new TDistribution(residualDF);
            final double interceptParam = betaVector.getEntry(0);
            final double interceptStdError = intercept.data().getDouble(0, Field.STD_ERROR);
            final double interceptTStat = interceptParam / interceptStdError;
            final double interceptPValue = distribution.cumulativeProbability(-Math.abs(interceptTStat)) * 2d;
            final double interceptCI = interceptStdError * distribution.inverseCumulativeProbability(1d - alpha / 2d);
            this.intercept.data().setDouble(0, Field.PARAMETER, interceptParam);
            this.intercept.data().setDouble(0, Field.T_STAT, interceptTStat);
            this.intercept.data().setDouble(0, Field.P_VALUE, interceptPValue);
            this.intercept.data().setDouble(0, Field.CI_LOWER, interceptParam - interceptCI);
            this.intercept.data().setDouble(0, Field.CI_UPPER, interceptParam + interceptCI);
            final int offset = hasIntercept() ? 1 : 0;
            for (int i=0; i<regressors.size(); ++i) {
                final C regressor = regressors.get(i);
                final double betaParam = betaVector.getEntry(i + offset);
                final double betaStdError = betas.data().getDouble(regressor, Field.STD_ERROR);
                final double tStat = betaParam / betaStdError;
                final double pValue = distribution.cumulativeProbability(-Math.abs(tStat)) * 2d;
                final double betaCI = betaStdError * distribution.inverseCumulativeProbability(1d - alpha / 2d);
                this.betas.data().setDouble(regressor, Field.PARAMETER, betaParam);
                this.betas.data().setDouble(regressor, Field.T_STAT, tStat);
                this.betas.data().setDouble(regressor, Field.P_VALUE, pValue);
                this.betas.data().setDouble(regressor, Field.CI_LOWER, betaParam - betaCI);
                this.betas.data().setDouble(regressor, Field.CI_UPPER, betaParam + betaCI);
            }
        } catch (Exception ex) {
            throw new DataFrameException("Failed to compute regression coefficient t-stats and p-values", ex);
        }
    }


    /**
     * Computes the Total Sum of Squares for regressand
     * @param y     the vector with dependent variable observations
     * @return      the Total Sum of Squares for regressand
     */
    protected double computeTSS(RealVector y) {
        if (!hasIntercept()) {
            return y.dotProduct(y);
        } else {
            final double[] values = y.toArray();
            final double mean = DoubleStream.of(values).average().orElse(Double.NaN);
            final double[] demeaned = DoubleStream.of(values).map(v -> v - mean).toArray();
            final RealVector demeanedVector = new ArrayRealVector(demeaned);
            return demeanedVector.dotProduct(demeanedVector);
        }
    }


    @Override
    public int getN() {
        return frame.rowCount();
    }


    @Override
    public C getRegressand() {
        return regressand;
    }


    @Override
    public boolean hasIntercept() {
        return hasIntercept;
    }


    @Override
    public List<C> getRegressors() {
        return Collections.unmodifiableList(regressors);
    }


    @Override
    public double getAlpha() {
        return alpha;
    }


    @Override
    public double getRSquared() {
        this.computeIf();
        return rSquared;
    }


    @Override
    public double getRSquaredAdj() {
        this.computeIf();
        return rSquaredAdj;
    }


    @Override
    public double getStdError() {
        this.computeIf();
        return stdError;
    }


    @Override
    public double getFValue() {
        this.computeIf();
        return fValue;
    }


    @Override
    public double getFValueProbability() {
        this.computeIf();
        return fValueProbability;
    }


    @Override
    public double getTotalSumOfSquares() {
        this.computeIf();
        return tss;
    }


    @Override
    public double getExplainedSumOfSquares() {
        this.computeIf();
        return ess;
    }


    @Override
    public double getResidualSumOfSquares() {
        this.computeIf();
        return rss;
    }


    @Override()
    public double getInterceptValue(Field field) {
        this.computeIf();
        return intercept.data().getDouble(0, field);
    }


    @Override
    public double getBetaValue(C regressor, Field field) {
        this.computeIf();
        return betas.data().getDouble(regressor, field);
    }


    @Override
    public DataFrame<C,Field> getBetas() {
        this.computeIf();
        return betas;
    }


    @Override
    public DataFrame<String,Field> getIntercept() {
        this.computeIf();
        return intercept;
    }


    @Override
    public DataFrame<R,String> getResiduals() {
        this.computeIf();
        return residuals;
    }


    @Override
    public DataFrame<R,String> getFittedValues() {
        try {
            this.computeIf();
            final double intercept = getInterceptValue(Field.PARAMETER);
            final double[] slopes = regressors.stream().mapToDouble(c -> getBetaValue(c, Field.PARAMETER)).toArray();
            return DataFrame.ofDoubles(frame.rows().keyArray(), Array.of("Fitted"), v -> {
                final R rowKey = v.rowKey();
                double fitted = intercept;
                for (int i=0; i<regressors.size(); ++i) {
                    final C regressor = regressors.get(i);
                    final double x = frame.data().getDouble(rowKey, regressor);
                    final double value = x * slopes[i];
                    fitted += value;
                }
                return fitted;
            });
        } catch (Exception ex) {
            throw new DataFrameException("Failed to compute regression fitted values", ex);
        }
    }


    @Override
    public double getDurbinWatsonStatistic() {
        try {
            this.computeIf();
            double etSquared = 0d;
            double deltaSquared = 0d;
            final int n = residuals.rowCount();
            for (int i=1; i<n; ++i) {
                final double error = residuals.data().getDouble(i, 0);
                final double errorPrevious = residuals.data().getDouble(i-1, 0);
                etSquared += error * error;
                deltaSquared += Math.pow(error - errorPrevious, 2);
            }
            return deltaSquared / etSquared;
        } catch (Exception ex) {
            throw new DataFrameException("Failed to compute the Durbin-Watson Statistic", ex);
        }
    }



    @Override
    public DataFrame<Integer,String> getResidualsAcf(int maxLag) {
        try {
            this.computeIf();
            final Range<Integer> lags = Range.of(0, maxLag);
            return DataFrame.ofDoubles(lags, Array.of("Residual(ACF)"), v -> {
                final int lag = v.rowKey();
                return residuals.colAt(0).stats().autocorr(lag);
            });
        } catch (Exception ex) {
            throw new DataFrameException("Failed to compute the autocorrelation function of residuals", ex);
        }
    }


    @Override
    public DataFrameLeastSquares<R,C> withSolver(Solver solver) {
        if (solver != this.solver) {
            this.solver = solver;
            this.residuals = null;
        }
        return this;
    }


    @Override
    public DataFrameLeastSquares<R,C> withAlpha(double alpha) {
        if (alpha != this.alpha) {
            this.alpha = alpha;
            this.residuals = null;
        }
        return this;
    }


    @Override
    public DataFrameLeastSquares<R,C> withIntercept(boolean intercept) {
        if (this.hasIntercept != intercept) {
            this.hasIntercept = intercept;
            this.residuals = null;
        }
        return this;
    }


    @Override
    public String toString() {
        this.computeIf();
        final int n = frame.rows().count();
        final int p = regressors.size() + 1;
        final int dfModel = regressors.size();
        final DataFrame<Object,Field> summary = getSummary();
        final String table = toText(100, summary);
        final int totalWidth = table.trim().split("\n")[1].length();
        final int cellWidth = (totalWidth - 4) / 4;
        final String title = "Linear Regression Results";
        final Format sciFormat = new DecimalFormat("0.###E0;-0.###E0");
        final String template = "%-" + cellWidth + "s%" + cellWidth + "s    %-" + cellWidth + "s%" + cellWidth + "s";

        final StringBuilder text = new StringBuilder();
        text.append("\n").append(lineOf('=', totalWidth));
        text.append("\n");
        text.append(lineOf(' ', (totalWidth / 2) - (title.length() / 2)));
        text.append(title);
        text.append(lineOf(' ', totalWidth - (totalWidth / 2) - (title.length() / 2) + title.length()));
        text.append("\n").append(lineOf('=', totalWidth));
        text.append("\n").append(String.format(template, "Model:", name, "R-Squared:", String.format("%.4f", getRSquared())));
        text.append("\n").append(String.format(template, "Observations:", n, "R-Squared(adjusted):", String.format("%.4f", getRSquaredAdj())));
        text.append("\n").append(String.format(template, "DF Model:", dfModel, "F-Statistic:", String.format("%.4f", getFValue())));
        text.append("\n").append(String.format(template, "DF Residuals:", n - p, "F-Statistic(Prob):", sciFormat.format(getFValueProbability())));
        text.append("\n").append(String.format(template, "Standard Error:", String.format("%.4f", getStdError()), "Runtime(millis)", runtimeMillis));
        text.append("\n").append(String.format(template, "Durbin-Watson:", String.format("%.4f", getDurbinWatsonStatistic()), "", ""));
        text.append("\n").append(lineOf('=', totalWidth));
        text.append(table);
        text.append("\n").append(lineOf('=', totalWidth));
        return text.toString();
    }


    /**
     * Generates a line generate from one char of the length specified
     * @param value     the character to draw line
     * @param length    the desired length
     * @return          the line
     */
    private String lineOf(char value, int length) {
        final char[] array = new char[length];
        Arrays.fill(array, value);
        return new String(array);
    }


    /**
     * Returns the summary DataFrame for this model
     * @return  the summary DataFrame for this model
     */
    @SuppressWarnings("unchecked")
    private DataFrame<Object,Field> getSummary() {
        final Stream<C> variables = regressors.stream();
        final Stream<String> intercept = hasIntercept() ? Stream.of("Intercept") : Stream.empty();
        final List<Object> rowKeys = Stream.concat(intercept, variables).collect(Collectors.toList());
        return DataFrame.ofDoubles(rowKeys, fields, value -> {
            final Field field = value.colKey();
            if (value.rowKey().equals("Intercept")) {
                return this.intercept.data().getDouble(0, field);
            } else {
                final C regressor = (C)value.rowKey();
                return betas.data().getDouble(regressor, field);
            }
        });
    }


    /**
     * Returns formatted text representing DataFrame content
     * @param rowCount  the max row count to include
     * @param frame     the frame reference
     * @return          to text
     */
    private String toText(int rowCount, DataFrame<?,Field> frame) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
        frame.out().print(rowCount, baos, formats -> {
            formats.setPrinter(Field.PARAMETER, Printer.ofDouble("#.####;-#.####"));
            formats.setPrinter(Field.STD_ERROR, Printer.ofDouble("#.####;-#.####"));
            formats.setPrinter(Field.T_STAT, Printer.ofDouble("#.####;-#.####"));
            formats.setPrinter(Field.P_VALUE, Printer.ofDouble("0.###E0;-0.###E0"));
            formats.setPrinter(Field.CI_LOWER, Printer.ofDouble("#.####;-#.####"));
            formats.setPrinter(Field.CI_UPPER, Printer.ofDouble("#.####;-#.####"));
        });
        return new String(baos.toByteArray());
    }
}
