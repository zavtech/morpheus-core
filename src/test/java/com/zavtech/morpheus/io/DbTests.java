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
package com.zavtech.morpheus.io;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.range.Range;
import com.zavtech.morpheus.util.functions.Function1;

/**
 * A unit test for database access
 *
 * @author  Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class DbTests {

    private static final File readDbDir = new File("src/test/resources/databases");
    private static final File testDir = new File(System.getProperty("java.io.tmpdir"), "databases");

    private enum DbType { H2, HSQL, SQLITE }

    private Map<String,DataSource> dataSourceMap = new LinkedHashMap<>();


    /**
     * Returns a newly created DataSource based on Apache Commons DBCP
     * @param dbType        the database type
     * @param path          the path to the database file
     * @return              the newly created data source
     */
    private static DataSource createDataSource(DbType dbType, File path) {
        System.out.println("Creating DataSource for " + path.getAbsolutePath());
        path.getParentFile().mkdirs();
        final BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDefaultAutoCommit(true);
        switch (dbType) {
            case H2:
                dataSource.setDriverClassName("org.h2.Driver");
                dataSource.setUrl("jdbc:h2://" + path.getAbsolutePath());
                dataSource.setUsername("sa");
                return dataSource;
            case HSQL:
                dataSource.setDriverClassName("org.hsqldb.jdbcDriver");
                dataSource.setUrl("jdbc:hsqldb:" + path.getAbsolutePath());
                dataSource.setUsername("sa");
                return dataSource;
            case SQLITE:
                dataSource.setDriverClassName("org.sqlite.JDBC");
                dataSource.setUrl("jdbc:sqlite:" + path.getAbsolutePath());
                dataSource.setUsername("");
                dataSource.setPassword("");
                return dataSource;

        }
        return dataSource;
    }



    @DataProvider(name = "readDatabases")
    public Object[][] readDatabases() {
        return new Object[][] {
            { "read/h2-db" },
            { "read/hsql-db" },
            { "read/sqlite-db" }
        };
    }


    @DataProvider(name = "writeDatabases")
    public Object[][] writeDatabases() {
        return new Object[][] {
            { "write/h2-db" },
            { "write/hsql-db" },
            { "write/sqlite-db" }
        };
    }


    @AfterClass
    public void dispose() {
        for (String key : dataSourceMap.keySet()) {
            try {
                final DataSource source = dataSourceMap.get(key);
                try (final Connection conn = source.getConnection()) {
                    final Statement stmt = conn.createStatement();
                    stmt.execute("shutdown");
                }
            } catch (Throwable t) {
                System.out.println("Failed to dispose connection for " + key);
            }
        }
    }


    /**
     * Recursively deletes files given a directory
     * @param file      the file or directory to delete
     */
    private static void delete(File file) {
        try {
            if (file.isDirectory()) {
                final File[] children = file.listFiles();
                if (children != null) {
                    for (File child : children) {
                        delete(child);
                    }
                }
            }
            System.out.println("Deleting " + file.getAbsolutePath());
            if (file.exists() && !file.delete()) {
                System.err.println("WARN: Failed to delete file: " + file.getAbsolutePath());
            }
        } catch (Exception ex) {
            System.err.println("Failed to delete file: " + file.getAbsolutePath());
        }
    }


    @BeforeClass
    public void setup() throws Exception {
        delete(testDir);
        dataSourceMap.put("read/h2-db", createDataSource(DbType.H2, new File(readDbDir, "h2-db/testDb")));
        dataSourceMap.put("read/hsql-db", createDataSource(DbType.HSQL, new File(readDbDir, "hsql-db/testDb")));
        dataSourceMap.put("read/sqlite-db", createDataSource(DbType.SQLITE, new File(readDbDir, "sqlite-db/testDb")));
        dataSourceMap.put("write/h2-db", createDataSource(DbType.H2, new File(testDir, "h2-db/testDb")));
        dataSourceMap.put("write/hsql-db", createDataSource(DbType.HSQL, new File(testDir, "hsql-db/testDb")));
        dataSourceMap.put("write/sqlite-db", createDataSource(DbType.SQLITE, new File(testDir, "sqlite-db/testDb")));
    }



    @Test(dataProvider="readDatabases")
    public void testRead1(String dbName) {
        final DataFrame<Integer,String> frame = DataFrame.read().db(options -> {
            options.withConnection(dataSourceMap.get(dbName));
            options.withSql("select * from \"ProcessLog\"");
        });
        Assert.assertEquals(frame.colCount(), 47);
        Assert.assertTrue(frame.cols().containsAll(Arrays.asList("Node", "ExecutablePath", "TerminationDate", "MinimumWorkingSetSize")));
        Assert.assertEquals(frame.rowCount(), 39);
        Assert.assertTrue(frame.rows().containsAll(Arrays.asList(1, 2, 3)));
        Assert.assertTrue(frame.colAt("ExecutablePath").toValueStream().anyMatch("C:\\Windows\\system32\\taskhost.exe"::equals));
        Assert.assertEquals(frame.colAt("TerminationDate").typeInfo(), LocalDate.class);
        Assert.assertEquals(frame.colAt("MinimumWorkingSetSize").typeInfo(), Long.class);
        frame.out().print();
    }


    @Test(dataProvider="readDatabases")
    public void testRead2(String dbName) {
        final DataFrame<String,String> frame = DataFrame.read().db(options -> {
            options.withConnection(dataSourceMap.get(dbName));
            options.withSql("select \"Ticker\", \"Fund Name\", \"Issuer\", \"AUM\", \"P/E\" from \"ETF\"");
        });
        Assert.assertEquals(frame.colCount(), 5);
        Assert.assertTrue(frame.cols().containsAll(Arrays.asList("Ticker", "Fund Name", "Issuer", "AUM", "P/E")));
        Assert.assertEquals(frame.rowCount(), 1685);
        Assert.assertTrue(frame.rows().containsAll(Arrays.asList("SPY", "QQQ", "IWD")));
        Assert.assertTrue(frame.colAt("Issuer").toValueStream().anyMatch("BlackRock"::equals));
        Assert.assertEquals(frame.colAt("Issuer").typeInfo(), String.class);
        Assert.assertEquals(frame.colAt("P/E").typeInfo(), Double.class);
        frame.out().print();
    }


    @Test(dataProvider="readDatabases")
    public void testRead3(String dbName) {
        final DataFrame<Integer,String> frame = DataFrame.read().db(options -> {
            options.withConnection(dataSourceMap.get(dbName));
            options.withSql("select * from \"TestTable\"");
        });
        Assert.assertEquals(frame.colCount(), 13);
        Assert.assertTrue(frame.cols().containsAll(Arrays.asList("Name", "Size", "Volume", "Price", "SomeDate", "SomeTimestamp")));
        Assert.assertEquals(frame.rowCount(), 3);
        Assert.assertTrue(frame.rows().containsAll(Arrays.asList(1, 2, 3)));
        Assert.assertTrue(frame.colAt("Name").toValueStream().anyMatch("Apple Computer"::equals));
        Assert.assertEquals(frame.colAt("Name").typeInfo(), String.class);
        Assert.assertEquals(frame.colAt("IsNew").typeInfo(), Boolean.class);
        Assert.assertEquals(frame.colAt("Size").typeInfo(), Double.class);
        Assert.assertEquals(frame.colAt("Price").typeInfo(), Double.class);
        Assert.assertEquals(frame.colAt("Volume").typeInfo(), Long.class);
        Assert.assertEquals(frame.colAt("SomeTimestamp").typeInfo(), LocalDateTime.class);
        Assert.assertEquals(frame.colAt("SomeDateTime").typeInfo(), LocalDateTime.class);
        Assert.assertEquals(frame.colAt("SomeDate").typeInfo(), LocalDate.class);
        Assert.assertEquals(frame.colAt("CharData").typeInfo(), String.class);
        Assert.assertEquals(frame.colAt("TinyIntData").typeInfo(), Integer.class);
        Assert.assertEquals(frame.colAt("SmallIntData").typeInfo(), Integer.class);
        Assert.assertEquals(frame.colAt("RealData").typeInfo(), Double.class);
        frame.out().print();
    }



    @Test(dataProvider = "writeDatabases")
    public void testEtfWrite(String dbName) {
        final DataSource source = dataSourceMap.get(dbName);
        final DataFrame<String,String> frame = DataFrame.read().csv(options -> {
            options.setResource("/csv/etf.csv");
            options.setExcludeColumns("Ticker");
            options.setColumnType("Geography", String.class);
            options.setRowKeyParser(String.class, values -> values[0]);
        });

        frame.rows().select(row -> row.key().equalsIgnoreCase("TDV")).out().print();
        frame.write().db(options -> {
            options.setConnection(source);
            options.setTableName("ETF");
            options.setRowKeyMapping("Ticker", String.class, Function1.toValue(v -> v));
            options.setBatchSize(1000);

        });

        frame.out().print();
    }


    @Test(dataProvider = "writeDatabases")
    public void testDatabaseWriteProcessLog(String dbName) {
        final DataFrame<Integer,String> frame = DataFrame.read().csv(options -> {
            options.setResource("/csv/process.csv");
            options.setExcludeColumns("PID");
            options.setRowKeyParser(Integer.class, values -> Integer.parseInt(values[25]));
            options.setCharset(StandardCharsets.UTF_16);
            options.setColumnType("ExecutionState", String.class);
            options.setColumnType("InstallDate", LocalDate.class);
            options.setColumnType("Status", String.class);
            options.setColumnType("TerminationDate", LocalDate.class);
            options.setColumnType("KernelModeTime", Long.class);
        });

        frame.write().db(options -> {
            options.setBatchSize(1000);
            options.setAutoIncrementColumnName("RecordId");
            options.setTableName("ProcessLog");
            options.setConnection(dataSourceMap.get(dbName));
        });
    }


    @Test(dataProvider = "writeDatabases")
    public void testWriteFollowedByRead(String dbName) {

        final DataFrame<Integer,String> frame1 = createRandomFrame(10);

        frame1.write().db(options -> {
            options.setBatchSize(1000);
            options.setTableName("RandomTable");
            options.setConnection(dataSourceMap.get(dbName));
            options.setAutoIncrementColumnName("RecordId");
        });

        final AtomicInteger counter = new AtomicInteger();
        final DataFrame<Integer,String> frame2 = DataFrame.read().db(options -> {
            options.withConnection(dataSourceMap.get(dbName));
            options.withSql("select * from \"RandomTable\"");
            options.withExcludeColumns("RecordId");
            options.withRowKeyFunction(rs -> counter.incrementAndGet());
        });

        frame1.out().print();
        frame2.out().print();
    }


    private DataFrame<Integer,String> createRandomFrame(int rowCount) {
        final Range<Integer> rowKeys = Range.of(0, rowCount);
        return DataFrame.of(rowKeys, String.class, columns -> {
            columns.add("Column-1", rowKeys.map(i -> i + 5));
            columns.add("Column-2", rowKeys.map(i -> LocalDate.now().plusDays(i)));
            columns.add("Column-3", rowKeys.map(i -> LocalDateTime.now().plusMinutes(i)));
            columns.add("Column-4", rowKeys.map(i -> "Value-" + i));
            columns.add("Column-5", rowKeys.map(i -> Math.random()));
            columns.add("Column-6", rowKeys.map(i -> Math.random() > 0.5));
            columns.add("Column-7", rowKeys.map(i -> new java.sql.Date(Instant.now().plusSeconds(i).toEpochMilli())));
            columns.add("Column-8", rowKeys.map(i -> new java.util.Date(Instant.now().plusSeconds(i).toEpochMilli())));
            columns.add("Column-9", rowKeys.map(i -> i * 10L));
        });
    }
}
