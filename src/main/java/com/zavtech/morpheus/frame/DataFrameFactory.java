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
package com.zavtech.morpheus.frame;

import java.lang.reflect.Constructor;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The factory class used to manufacture new <code>DataFrame</code> instances in various ways.
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public abstract class DataFrameFactory {

    private static Throwable throwable;
    private static DataFrameFactory instance;

    /**
     * Static initializer
     */
    static {
        final String defaultClassName = "com.zavtech.morpheus.reference.XDataFrameFactory";
        try {
            final String className = System.getProperty("dataFrameFactory", defaultClassName);
            final Class factoryClass = Class.forName(className);
            DataFrameFactory.setFactoryClass(factoryClass);
        } catch (Throwable t) {
            throwable = t;
            System.err.println("Failed to initialize DataFrameFactory");
            t.printStackTrace();
            try {
                DataFrameFactory.setFactoryClass(Class.forName(defaultClassName));
            } catch (Exception ex) {
                throwable = t;
                System.err.println("Failed to initialize reference implementation of DataFrameFactory");
                ex.printStackTrace();
            }
        }
    }

    /**
     * Constructor
     */
    protected DataFrameFactory() {
        super();
    }

    /**
     * Sets the implementation factory class to use
     * Note that the factory class must have a public no-arg constructor
     * @param factoryClass  the factory class
     */
    @SuppressWarnings("unchecked")
    public static synchronized void setFactoryClass(Class factoryClass) {
        try {
            final Constructor constructor = factoryClass.getDeclaredConstructor();
            constructor.setAccessible(true);
            instance = (DataFrameFactory)constructor.newInstance();
        } catch (Throwable t) {
            throw new DataFrameException("Unable to initial DataFrameFactory for class: " + factoryClass, t);
        }
    }

    /**
     * Returns the singleton instance of the DataFrameCreate
     * @return  the singleton factory
     */
    public static DataFrameFactory getInstance() {
        if (instance != null) {
            return instance;
        } else {
            throw new IllegalStateException("The DataFrameFactory class failed to initialize", throwable);
        }
    }

    /**
     * Returns a reference to the DataFrame read interface
     * @return  the DataFrame read interface
     */
    public abstract DataFrameRead read();

    /**
     * Returns an empty DataFrame with zero length rows and columns
     * @param <R>           the row key type
     * @param <C>           the column key type
     * @return              the empty DataFrame
     */
    public abstract <R,C> DataFrame<R,C> empty();

    /**
     * Returns an empty DataFrame with zero length rows and columns
     * @param rowAxisType   the row axis key type
     * @param colAxisType   the column axis key type
     * @param <R>           the row key type
     * @param <C>           the column key type
     * @return              the empty DataFrame
     */
    public abstract <R,C> DataFrame<R,C> empty(Class<R> rowAxisType, Class<C> colAxisType);

    /**
     * Returns a DataFrame result by combining multiple frames into one while applying only the first non-null element value
     * If there are intersecting coordinates across the frames, the first non-null value will apply in the resulting frame
     * @param frames    the iterator of frames
     * @param <R>       the row key type
     * @param <C>       the column key type
     * @return          the resulting DataFrame
     */
    public abstract <R,C> DataFrame<R,C> combineFirst(Iterator<DataFrame<R,C>> frames);

    /**
     * Returns a newly created DataFrame by concatenating rows from the input frames
     * If there are overlapping row keys, the row values from the first frame will apply
     * @param frames        the iterable of frames from which to concatenate rows
     * @param <R>           the row key type for frames
     * @param <C>           the column key type for frames
     * @return              the resulting DataFrame
     */
    public abstract <R,C> DataFrame<R,C> concatRows(Iterator<DataFrame<R,C>> frames);

    /**
     * Returns a newly created DataFrame by concatenating columns from the input frames
     * If there are overlapping column keys, the column values from the first frame will apply
     * @param frames        the iterable of frames from which to concatenate columns
     * @param <R>           the row key type for frames
     * @param <C>           the column key type for frames
     * @return              the resulting DataFrame
     */
    public abstract <R,C> DataFrame<R,C> concatColumns(Iterator<DataFrame<R,C>> frames);

    /**
     * Returns a newly created empty <code>DataFrame</code> initialized with the row and column type
     * @param rowType       the row key type
     * @param colType       the column key type
     * @param <R>           the row key type
     * @param <C>           the column key type
     * @return              the newly created <code>DataFrame</code>
     */
    public abstract <R,C> DataFrame<R,C> from(Class<R> rowType, Class<C> colType);

    /**
     * Returns a newly created <code>DataFrame</code> initialized with the row and column keys specified
     * @param rowKeys       the row keys for frame
     * @param colKeys       the column keys for frame
     * @param type          the data type for columns
     * @param <R>           the row key type
     * @param <C>           the column key type
     * @return              the newly created <code>DataFrame</code>
     */
    public abstract <R,C> DataFrame<R,C> from(Iterable<R> rowKeys, Iterable<C> colKeys, Class<?> type);

    /**
     * Returns a newly created <code>DataFrame</code> initialized with rows and any state added by the consumer
     * @param rowKeys       the row keys for frame
     * @param colType       the column key type
     * @param columns       the consumer which can be used to configure columns
     * @param <R>           the row key type
     * @param <C>           the column key type
     * @return              the newly created <code>DataFrame</code>
     */
    public abstract <R,C> DataFrame<R,C> from(Iterable<R> rowKeys, Class<C> colType, Consumer<DataFrameColumns<R,C>> columns);

    /**
     * Returns a newly created <code>DataFrame</code> initialized with rows and any state added by the consumer
     * @param rowType       the row key type for frame
     * @param colType       the column key type
     * @param columns       the consumer which can be used to configure columns
     * @param <R>           the row key type
     * @param <C>           the column key type
     * @return              the newly created <code>DataFrame</code>
     */
    public abstract <R,C> DataFrame<R,C> from(Class<R> rowType, Class<C> colType, Consumer<DataFrameColumns<R,C>> columns);

    /**
     * Returns a newly created <code>DataFrame</code> initialized with data from a SQL ResultSet
     * @param resultSet     the result set to initialize from
     * @param rowCapacity   the expected row count for <code>DataFrame</code>
     * @param <R>           the row key type
     * @throws SQLException if fails to initialize from result set
     * @return              the newly created <code>DataFrame</code>
     */
    public abstract <R> DataFrame<R,String> from(ResultSet resultSet, int rowCapacity, Function<ResultSet,R> rowKeyFunction) throws SQLException;

}
