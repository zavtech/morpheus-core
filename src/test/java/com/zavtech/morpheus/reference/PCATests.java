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

import java.io.File;
import java.net.URL;
import java.util.Optional;
import java.util.stream.IntStream;

import org.junit.Test;
import org.testng.Assert;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameException;
import com.zavtech.morpheus.frame.DataFramePCA;

/**
 * Unit tests for Principal Component Analysis
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class PCATests {


    DataFrame<Integer,Integer> poppet() {
        final URL url = getClass().getResource("/pca/poppet.jpg");
        final DataFrame<Integer,Integer> data = DataFrame.ofImage(url);
        Assert.assertEquals(data.rowCount(), 360);
        Assert.assertEquals(data.colCount(), 504);
        return data;
    }


    @Test()
    public void pcaWithSVD() {

        //Transpose image to nxp DataFrame where n > p since Morpheus PCA operates on this assumption
        final DataFrame<Integer,Integer> data = poppet().transpose().mapToDoubles(v -> v.getDouble());

        data.pca().apply(true, DataFramePCA.Solver.SVD, model -> {

            String file1 = "./src/test/resources/pca/svd/poppet-svd-eigenvalues.csv";
            String file2 = "./src/test/resources/pca/svd/poppet-svd-eigenvectors.csv";

            //Check eigenvalues matched previously recorded data
            final DataFrame<Integer,String> actualEigenValues = model.getEigenValues().cols().mapKeys(c -> c.key().toString());
            Assert.assertEquals(actualEigenValues.colCount(), 3);
            Assert.assertEquals(actualEigenValues.rowCount(), 360);
            DataFrameAsserts.assertEqualsByIndex(actualEigenValues, DataFrame.read().csv(options -> {
                options.setResource(file1);
            }));

            //Check eigenvectors matched previously recorded data
            final DataFrame<Integer,String> actualEigenVectors = model.getEigenVectors().cols().mapKeys(c -> String.valueOf(c.key()));
            Assert.assertEquals(actualEigenVectors.colCount(), 360);
            Assert.assertEquals(actualEigenVectors.rowCount(), 360);
            DataFrameAsserts.assertEqualsByIndex(actualEigenVectors, DataFrame.read().csv(options -> {
                options.setResource(file2);
            }));

            IntStream.of(30, 50, 100).forEach(nComps -> {

                String scoreFile = "./src/test/resources/pca/svd/poppet-svd-scores-" + nComps + ".csv";
                String projectionFile = "./src/test/resources/pca/svd/poppet-svd-projection-" + nComps + ".csv";

                final DataFrame<Integer,String> actualScores = model.getScores(nComps).cols().mapKeys(c -> String.valueOf(c.key()));
                Assert.assertEquals(actualScores.rowCount(), 504);
                Assert.assertEquals(actualScores.colCount(), nComps);
                DataFrameAsserts.assertEqualsByIndex(actualScores, DataFrame.read().csv(options -> {
                    options.setResource(scoreFile);
                }));


                final DataFrame<Integer,String> actualProjection = model.getProjection(nComps).cols().mapKeys(c -> String.valueOf(c.key()));
                Assert.assertEquals(actualProjection.rowCount(), 504);
                Assert.assertEquals(actualProjection.colCount(), 360);
                DataFrameAsserts.assertEqualsByIndex(actualProjection, DataFrame.read().csv(options -> {
                    options.setResource(projectionFile);
                }));

            });
            return Optional.empty();
        });
    }


    @Test()
    public void pcaWithEVD_COV() {

        //Transpose image to nxp DataFrame where n > p since Morpheus PCA operates on this assumption
        final DataFrame<Integer,Integer> data = poppet().transpose().mapToDoubles(v -> v.getDouble());

        data.pca().apply(true, DataFramePCA.Solver.EVD_COV, model -> {

            model.getEigenValues().out().print();
            model.getEigenVectors().out().print();

            String file1 = "./src/test/resources/pca/evd_cov/poppet-evd-eigenvalues.csv";
            String file2 = "./src/test/resources/pca/evd_cov/poppet-evd-eigenvectors.csv";

            //Check eigenvalues matched previously recorded data
            final DataFrame<Integer,String> actualEigenValues = model.getEigenValues().cols().mapKeys(c -> c.key().toString());
            Assert.assertEquals(actualEigenValues.colCount(), 3);
            Assert.assertEquals(actualEigenValues.rowCount(), 360);
            DataFrameAsserts.assertEqualsByIndex(actualEigenValues, DataFrame.read().csv(options -> {
                options.setResource(file1);
            }));

            //Check eigenvectors matched previously recorded data
            final DataFrame<Integer,String> actualEigenVectors = model.getEigenVectors().cols().mapKeys(c -> String.valueOf(c.key()));
            Assert.assertEquals(actualEigenVectors.colCount(), 360);
            Assert.assertEquals(actualEigenVectors.rowCount(), 360);
            DataFrameAsserts.assertEqualsByIndex(actualEigenVectors, DataFrame.read().csv(options -> {
                options.setResource(file2);
            }));

            IntStream.of(30, 50, 100).forEach(nComps -> {

                String scoreFile = "./src/test/resources/pca/evd_cov/poppet-evd-scores-" + nComps + ".csv";
                String projectionFile = "./src/test/resources/pca_cov/evd/poppet-evd-projection-" + nComps + ".csv";

                final DataFrame<Integer,String> actualScores = model.getScores(nComps).cols().mapKeys(c -> String.valueOf(c.key()));
                Assert.assertEquals(actualScores.rowCount(), 504);
                Assert.assertEquals(actualScores.colCount(), nComps);
                DataFrameAsserts.assertEqualsByIndex(actualScores, DataFrame.read().csv(options -> {
                    options.setResource(scoreFile);
                }));

                final DataFrame<Integer,String> actualProjection = model.getProjection(nComps).cols().mapKeys(c -> String.valueOf(c.key()));
                Assert.assertEquals(actualProjection.rowCount(), 504);
                Assert.assertEquals(actualProjection.colCount(), 360);
                DataFrameAsserts.assertEqualsByIndex(actualProjection, DataFrame.read().csv(options -> {
                    options.setResource(projectionFile);
                }));
            });
            return Optional.empty();
        });
    }


    @Test()
    public void pcaWithEVD_COR() {

        //Transpose image to nxp DataFrame where n > p since Morpheus PCA operates on this assumption
        final DataFrame<Integer,Integer> data = poppet().transpose().mapToDoubles(v -> v.getDouble());

        data.pca().apply(true, DataFramePCA.Solver.EVD_COV, model -> {

            model.getEigenValues().out().print();
            model.getEigenVectors().out().print();

            String file1 = "./src/test/resources/pca/evd_corr/poppet-evd-eigenvalues.csv";
            String file2 = "./src/test/resources/pca/evd_corr/poppet-evd-eigenvectors.csv";

            //Check eigenvalues matched previously recorded data
            final DataFrame<Integer,String> actualEigenValues = model.getEigenValues().cols().mapKeys(c -> c.key().toString());
            Assert.assertEquals(actualEigenValues.colCount(), 3);
            Assert.assertEquals(actualEigenValues.rowCount(), 360);
            DataFrameAsserts.assertEqualsByIndex(actualEigenValues, DataFrame.read().csv(options -> {
                options.setResource(file1);
            }));

            //Check eigenvectors matched previously recorded data
            final DataFrame<Integer,String> actualEigenVectors = model.getEigenVectors().cols().mapKeys(c -> String.valueOf(c.key()));
            Assert.assertEquals(actualEigenVectors.colCount(), 360);
            Assert.assertEquals(actualEigenVectors.rowCount(), 360);
            DataFrameAsserts.assertEqualsByIndex(actualEigenVectors, DataFrame.read().csv(options -> {
                options.setResource(file2);
            }));

            IntStream.of(30, 50, 100).forEach(nComps -> {

                String scoreFile = "./src/test/resources/pca/evd_corr/poppet-evd-scores-" + nComps + ".csv";
                String projectionFile = "./src/test/resources/pca/evd_corr/poppet-evd-projection-" + nComps + ".csv";

                final DataFrame<Integer,String> actualScores = model.getScores(nComps).cols().mapKeys(c -> String.valueOf(c.key()));
                Assert.assertEquals(actualScores.rowCount(), 504);
                Assert.assertEquals(actualScores.colCount(), nComps);
                DataFrameAsserts.assertEqualsByIndex(actualScores, DataFrame.read().csv(options -> {
                    options.setResource(scoreFile);
                }));

                final DataFrame<Integer,String> actualProjection = model.getProjection(nComps).cols().mapKeys(c -> String.valueOf(c.key()));
                Assert.assertEquals(actualProjection.rowCount(), 504);
                Assert.assertEquals(actualProjection.colCount(), 360);
                DataFrameAsserts.assertEqualsByIndex(actualProjection, DataFrame.read().csv(options -> {
                    options.setResource(projectionFile);
                }));
            });
            return Optional.empty();
        });
    }


    @Test(expected = DataFrameException.class)
    public void testFailureOnWrongDimensionsSVD() {
        final DataFrame<Integer,Integer> data = poppet().mapToDoubles(v -> v.getDouble());
        data.pca().apply(true, DataFramePCA.Solver.EVD_COV, model -> {
            model.getScores(30).out().print();
            model.getProjection(30).out().print();
            return Optional.empty();
        });
    }


    @Test(expected = DataFrameException.class)
    public void testFailureOnWrongDimensionsEVD() {
        final DataFrame<Integer,Integer> data = poppet().mapToDoubles(v -> v.getDouble());
        data.pca().apply(true, DataFramePCA.Solver.SVD, model -> {
            model.getScores(30).out().print();
            model.getProjection(30).out().print();
            return Optional.empty();
        });
    }

}
