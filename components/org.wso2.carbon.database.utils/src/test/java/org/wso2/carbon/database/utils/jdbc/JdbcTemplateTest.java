/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.database.utils.jdbc;

import org.apache.commons.dbcp.BasicDataSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.wso2.carbon.database.utils.jdbc.exceptions.DataAccessException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.anyString;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 * This class contains the test cases for JDBCTemplate without the transaction support.
 */
@PrepareForTest({BasicDataSource.class})
public class JdbcTemplateTest extends PowerMockTestCase {

    private static final String DB_NAME = "TestDB";
    private static final String INSERT_QUERY = "INSERT INTO PURPOSE(NAME,DESCRIPTION) VALUES(?,?)";
    private static final String INSERT_WITH_VALUES_QUERY = "INSERT INTO PURPOSE(NAME,DESCRIPTION) VALUES('Exec name'," +
            "'Exec des')";
    private static final String INSERT_WITH_ERROR_QUERY = "INSERT INTO PURPOSE(NAME,DESCRIPTION) VALUES('Exec name'," +
            "'Exec des') WHERE ID=?";
    private static final String INSERT_WITH_EXEC_ERROR_QUERY = "INSERT INTO PURPOSE(ID,NAME,DESCRIPTION) VALUES(1," +
            "'Exec name','Exec des')";
    private static final String SELECT_QUERY = "SELECT * FROM PURPOSE;";
    private static final String SELECT_WITH_FILTER_QUERY = "SELECT * FROM PURPOSE WHERE ID=?";
    private static final String SELECT_FROM_NAME_QUERY = "SELECT * FROM PURPOSE WHERE NAME=?";
    private static final String ERROR_QUERY = "SELECT";
    private static final String DELETE_WITH_FILTER_QUERY = "DELETE FROM PURPOSE WHERE ID=?";
    private static final String DELETE_QUERY = "DELETE FROM PURPOSE";

    @Mock
    private BasicDataSource mockedDataSource;

    @Mock
    private ResultSet mockedRes;

    private BasicDataSource basicDataSource;

    @DataProvider(name = "exceptionLevelProvider")
    public static Object[][] provideExceptionLevels() {

        return new Object[][]{
                {1},
                {2}
        };
    }

    @DataProvider(name = "autoCommitLevelProvider")
    public static Object[][] provideAutoCommitLevels() {

        return new Object[][]{
                {1},
                {2}
        };
    }

    private static PurposeDummy mapRow(ResultSet resultSet, int rowNumber) throws SQLException {

        PurposeDummy purpose = new PurposeDummy();
        purpose.setId(resultSet.getInt(1));
        purpose.setName(resultSet.getString(2));
        purpose.setDescription(resultSet.getString(3));
        return purpose;
    }

    @BeforeMethod
    public void setUp() throws Exception {

        JdbcTemplateTestUtils templateTestUtils = new JdbcTemplateTestUtils();
        templateTestUtils.initH2Database(DB_NAME, JdbcTemplateTestUtils.getFilePath("h2.sql"));
        basicDataSource = templateTestUtils.getDataSource();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        jdbcTemplate.executeInsert(INSERT_QUERY, preparedStatement -> {
            preparedStatement.setString(1, "Initial dummy name");
            preparedStatement.setString(2, "Initial dummy description");
        }, null, false);

        jdbcTemplate.executeInsert(INSERT_QUERY, preparedStatement -> {
            preparedStatement.setString(1, "Initial dummy name");
            preparedStatement.setString(2, "Initial dummy description");
        }, null, false);
    }

    @Test
    public void testExecuteQuery() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        List<PurposeDummy> purposeList = jdbcTemplate.executeQuery(SELECT_QUERY, JdbcTemplateTest::mapRow);
        assertEquals(purposeList.get(0).getName(), "Initial dummy name");
        assertEquals(purposeList.get(1).getName(), "Initial dummy name");
    }

    @Test
    public void testExecuteQueryWithQueryFilter() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        List<PurposeDummy> purposeList = jdbcTemplate.executeQuery(SELECT_WITH_FILTER_QUERY, JdbcTemplateTest::mapRow,
                preparedStatement -> preparedStatement.setInt(1, 1));
        assertEquals(purposeList.get(0).getName(), "Initial dummy name");
    }

    @Test(expectedExceptions = DataAccessException.class)
    public void testExecuteQueryWithException() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        jdbcTemplate.executeQuery(ERROR_QUERY, JdbcTemplateTest::mapRow,
                preparedStatement -> preparedStatement.setInt(1, 1));
    }

    @Test(expectedExceptions = DataAccessException.class)
    public void testExecuteQueryWithConnectionError() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(mockedDataSource);
        when(mockedDataSource.getConnection()).thenThrow(new SQLException());
        jdbcTemplate.executeQuery(SELECT_WITH_FILTER_QUERY, JdbcTemplateTest::mapRow, preparedStatement
                -> preparedStatement.setInt(1, 1));
    }

    @Test
    public void testExecuteQueryWithAutoCommitFalse() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(mockedDataSource);
        Connection connection = Mockito.spy(basicDataSource.getConnection());
        connection.setAutoCommit(false);
        when(mockedDataSource.getConnection()).thenReturn(connection);

        List<PurposeDummy> purposeList = jdbcTemplate.executeQuery(SELECT_QUERY, JdbcTemplateTest::mapRow);
        assertEquals(purposeList.get(0).getName(), "Initial dummy name");
        assertEquals(purposeList.get(1).getName(), "Initial dummy name");
    }

    @Test(expectedExceptions = DataAccessException.class)
    public void testExecuteQueryWithPrepStatError() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(mockedDataSource);
        Connection connection = Mockito.spy(basicDataSource.getConnection());
        PreparedStatement prepStat = Mockito.spy(connection.prepareStatement(SELECT_QUERY));
        when(mockedDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(prepStat);
        when(prepStat.executeQuery()).thenThrow(new SQLException());

        jdbcTemplate.executeQuery(SELECT_QUERY, JdbcTemplateTest::mapRow);
    }

    @Test
    public void testFetchSingleRecord() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        PurposeDummy purposeDummy = jdbcTemplate.fetchSingleRecord(SELECT_WITH_FILTER_QUERY, JdbcTemplateTest::mapRow,
                preparedStatement -> preparedStatement.setInt(1, 1));
        assertEquals(purposeDummy.getName(), "Initial dummy name");
    }

    @Test(expectedExceptions = DataAccessException.class)
    public void testFetchSingleRecordWithNoQueryFilterWithException() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        jdbcTemplate.fetchSingleRecord(SELECT_WITH_FILTER_QUERY, JdbcTemplateTest::mapRow, null);
    }

    @Test
    public void testFetchSingleRecordWithNoResult() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        PurposeDummy purposeDummy = jdbcTemplate.fetchSingleRecord(SELECT_WITH_FILTER_QUERY, JdbcTemplateTest::mapRow,
                preparedStatement -> preparedStatement.setInt(1, -1));
        assertNull(purposeDummy);
    }

    @Test(expectedExceptions = DataAccessException.class)
    public void testFetchSingleRecordWithMultipleResults() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        jdbcTemplate.fetchSingleRecord(SELECT_FROM_NAME_QUERY, JdbcTemplateTest::mapRow,
                preparedStatement -> preparedStatement.setString(1, "Initial dummy name"));
    }

    @Test(expectedExceptions = DataAccessException.class)
    public void testFetchSingleRecordWithException() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        jdbcTemplate.fetchSingleRecord(ERROR_QUERY, JdbcTemplateTest::mapRow,
                preparedStatement -> preparedStatement.setInt(1, 1));
    }

    @Test
    public void testExecuteUpdate() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        jdbcTemplate.executeUpdate(DELETE_WITH_FILTER_QUERY,
                preparedStatement -> preparedStatement.setInt(1, 2));
    }

    @Test(expectedExceptions = DataAccessException.class)
    public void testExecuteUpdateWithException() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        jdbcTemplate.executeUpdate(DELETE_QUERY, preparedStatement -> preparedStatement.setInt(1, 0));
    }

    @Test
    public void testExecuteUpdateWithNoQueryFilter() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        jdbcTemplate.executeUpdate(INSERT_WITH_VALUES_QUERY, null);
    }

    @Test(expectedExceptions = DataAccessException.class)
    public void testExecuteUpdateWithExecError() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        jdbcTemplate.executeUpdate(ERROR_QUERY, null);
    }

    @Test
    public void testExecuteUpdateWithAutoCommitFalse() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(mockedDataSource);
        Connection connection = Mockito.spy(basicDataSource.getConnection());
        connection.setAutoCommit(false);
        when(mockedDataSource.getConnection()).thenReturn(connection);

        jdbcTemplate.executeUpdate(DELETE_WITH_FILTER_QUERY,
                preparedStatement -> preparedStatement.setInt(1, 2));
    }

    @Test
    public void testExecuteUpdateWithOnlyQuery() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        int id = jdbcTemplate.executeUpdate(INSERT_WITH_VALUES_QUERY);
        assertNotEquals(id, 0);
    }

    @Test
    public void testExecuteUpdateWithOnlyQueryAndAutoCommitFalse() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(mockedDataSource);
        Connection connection = Mockito.spy(basicDataSource.getConnection());
        connection.setAutoCommit(false);
        when(mockedDataSource.getConnection()).thenReturn(connection);
        int id = jdbcTemplate.executeUpdate(INSERT_WITH_VALUES_QUERY);
        assertNotEquals(id, 0);
    }

    @Test(expectedExceptions = DataAccessException.class)
    public void testExecuteUpdateWithOnlyQueryWithException() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        jdbcTemplate.executeUpdate(ERROR_QUERY);
    }

    @Test(expectedExceptions = DataAccessException.class)
    public void testExecuteUpdateWithOnlyQueryWithConnectionError() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(mockedDataSource);
        when(mockedDataSource.getConnection()).thenThrow(new SQLException());
        jdbcTemplate.executeUpdate(INSERT_WITH_VALUES_QUERY);
    }

    @Test(expectedExceptions = DataAccessException.class)
    public void testExecuteUpdateWithOnlyQueryWithDoInternalError() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        jdbcTemplate.executeUpdate(INSERT_WITH_EXEC_ERROR_QUERY);
    }

    @Test
    public void testExecuteInsert() throws Exception {

        PurposeDummy purpose = new PurposeDummy("Test Name 001", "Test Description 001");
        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        int id = jdbcTemplate.executeInsert(INSERT_QUERY, (preparedStatement) -> {
            preparedStatement.setString(1, purpose.getName());
            preparedStatement.setString(2, purpose.getDescription());
        }, purpose, true);
        assertNotEquals(id, 0);
    }

    @Test
    public void testExecuteInsertWithFetchInsertedIdFalse() throws Exception {

        PurposeDummy purpose = new PurposeDummy("Test Name 001", "Test Description 001");
        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        int id = jdbcTemplate.executeInsert(INSERT_QUERY, (preparedStatement) -> {
            preparedStatement.setString(1, purpose.getName());
            preparedStatement.setString(2, purpose.getDescription());
        }, purpose, false);
        assertEquals(0, id);
    }

    @Test(expectedExceptions = DataAccessException.class)
    public void testExecuteInsertWithConnectionError() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(mockedDataSource);
        when(mockedDataSource.getConnection()).thenThrow(new SQLException());
        PurposeDummy purpose = new PurposeDummy("Test Name 001", "Test Description 001");
        jdbcTemplate.executeInsert(INSERT_QUERY, (preparedStatement) -> {
            preparedStatement.setString(1, purpose.getName());
            preparedStatement.setString(2, purpose.getDescription());
        }, purpose, true);
    }

    @Test(expectedExceptions = DataAccessException.class)
    public void testExecuteInsertWithExecError() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        jdbcTemplate.executeInsert(INSERT_WITH_ERROR_QUERY,
                (preparedStatement) -> preparedStatement.setInt(1, 1), null, true);
    }

    @Test(expectedExceptions = DataAccessException.class)
    public void testExecuteInsertWithDoInternalUpdateError() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        PurposeDummy purpose = new PurposeDummy("Test Name 001", "Test Description 001");
        jdbcTemplate.executeInsert(INSERT_QUERY, (preparedStatement) -> {
            preparedStatement.setString(1, purpose.getName());
            preparedStatement.setString(2, null);
        }, purpose, true);
    }

    @Test(dataProvider = "autoCommitLevelProvider")
    public void testExecuteInsertWithAutoCommitFalse(int autoCommitLevel) throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(mockedDataSource);
        Connection connection = Mockito.spy(basicDataSource.getConnection());
        connection.setAutoCommit(false);
        when(mockedDataSource.getConnection()).thenReturn(connection);
        PurposeDummy purpose = new PurposeDummy("Test Name 001", "Test Description 001");

        if (autoCommitLevel == 1) {
            int id = jdbcTemplate.executeInsert(INSERT_QUERY, (preparedStatement) -> {
                preparedStatement.setString(1, purpose.getName());
                preparedStatement.setString(2, purpose.getDescription());
            }, purpose, true);
            assertNotEquals(id, 0);
        } else if (autoCommitLevel == 2) {
            int id = jdbcTemplate.executeInsert(INSERT_QUERY, (preparedStatement) -> {
                preparedStatement.setString(1, purpose.getName());
                preparedStatement.setString(2, purpose.getDescription());
            }, purpose, false);
            assertEquals(id, 0);
        }
    }

    @Test(expectedExceptions = DataAccessException.class)
    public void testExecuteInsertWithGenIdError() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(mockedDataSource);
        Connection connection = Mockito.spy(basicDataSource.getConnection());
        PreparedStatement prepStat = Mockito.spy(connection.prepareStatement(INSERT_QUERY));
        connection.setAutoCommit(false);
        when(mockedDataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString(), Mockito.any(String[].class))).thenReturn(prepStat);
        when(prepStat.getGeneratedKeys()).thenReturn(mockedRes);

        PurposeDummy purpose = new PurposeDummy("Test Name 001", "Test Description 001");
        jdbcTemplate.executeInsert(INSERT_QUERY, (preparedStatement) -> {
            preparedStatement.setString(1, purpose.getName());
            preparedStatement.setString(2, purpose.getDescription());
        }, purpose, true);
    }

    @Test(expectedExceptions = DataAccessException.class)
    public void testExecuteInsertWithNoFetchAndException() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        PurposeDummy purpose = new PurposeDummy("Test Name 001", "Test Description 001");
        jdbcTemplate.executeInsert(INSERT_QUERY, (preparedStatement) -> {
            preparedStatement.setString(1, purpose.getName());
            preparedStatement.setString(2, null);
        }, purpose, false);
    }

    @Test
    public void testExecuteBatchInsert() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        HashMap<String, String> testMap = new HashMap<>();
        testMap.put("testName1", "testDes1");
        testMap.put("testName2", "testDes2");
        testMap.put("testName3", "testDes3");

        jdbcTemplate.executeBatchInsert(INSERT_QUERY, preparedStatement -> {
            for (Map.Entry<String, String> entry : testMap.entrySet()) {
                preparedStatement.setString(1, entry.getKey());
                preparedStatement.setString(2, entry.getValue());
                preparedStatement.addBatch();
            }
        }, "testBean");
    }

    @Test
    public void testExecuteBatchInsertWithAutoCommitFalse() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(mockedDataSource);
        Connection connection = Mockito.spy(basicDataSource.getConnection());
        connection.setAutoCommit(false);
        when(mockedDataSource.getConnection()).thenReturn(connection);

        HashMap<String, String> testMap = new HashMap<>();
        testMap.put("testName1", "testDes1");
        testMap.put("testName2", "testDes2");
        testMap.put("testName3", "testDes3");

        jdbcTemplate.executeBatchInsert(INSERT_QUERY, preparedStatement -> {
            for (Map.Entry<String, String> entry : testMap.entrySet()) {
                preparedStatement.setString(1, entry.getKey());
                preparedStatement.setString(2, entry.getValue());
                preparedStatement.addBatch();
            }
        }, "testBean");
    }

    @Test(expectedExceptions = DataAccessException.class)
    public void testExecuteBatchInsertWithErrorQuery() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        jdbcTemplate.executeBatchInsert(ERROR_QUERY, null, null);
    }

    @Test(expectedExceptions = DataAccessException.class)
    public void testExecuteBatchInsertWithErrorConnection() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(mockedDataSource);
        when(mockedDataSource.getConnection()).thenThrow(new SQLException());
        HashMap<String, String> testMap = new HashMap<>();
        testMap.put("testName1", "testDes1");
        jdbcTemplate.executeBatchInsert(INSERT_QUERY, null, null);
    }

    @Test(expectedExceptions = DataAccessException.class)
    public void testExecuteBatchInsertWithDoInternalError() throws Exception {

        HashMap<Integer, String> testMap = new HashMap<>();
        testMap.put(1, "testDes1");
        testMap.put(2, "testDes2");

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        jdbcTemplate.executeBatchInsert(INSERT_WITH_EXEC_ERROR_QUERY, preparedStatement -> {
            for (Map.Entry<Integer, String> entry : testMap.entrySet()) {
                preparedStatement.setInt(1, entry.getKey());
                preparedStatement.setString(2, entry.getValue());
            }
        }, "testBean");
    }

    @Test
    public void testExecuteBatchUpdateWithResults() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        HashMap<String, String> testMap = new HashMap<>();
        testMap.put("testName1", "testDes1");
        testMap.put("testName2", "testDes2");
        testMap.put("testName3", "testDes3");

        int[] results = jdbcTemplate.executeBatchUpdateWithResults(INSERT_QUERY, preparedStatement -> {
            for (Map.Entry<String, String> entry : testMap.entrySet()) {
                preparedStatement.setString(1, entry.getKey());
                preparedStatement.setString(2, entry.getValue());
                preparedStatement.addBatch();
            }
        });
        assertEquals(results.length, 3);
        for (int result : results) {
            assertEquals(result, 1);
        }
    }

    @Test
    public void testExecuteBatchUpdateWithResultsWithAutoCommitFalse() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(mockedDataSource);
        Connection connection = Mockito.spy(basicDataSource.getConnection());
        connection.setAutoCommit(false);
        when(mockedDataSource.getConnection()).thenReturn(connection);

        HashMap<String, String> testMap = new HashMap<>();
        testMap.put("testName1", "testDes1");
        testMap.put("testName2", "testDes2");
        testMap.put("testName3", "testDes3");

        int[] results = jdbcTemplate.executeBatchUpdateWithResults(INSERT_QUERY, preparedStatement -> {
            for (Map.Entry<String, String> entry : testMap.entrySet()) {
                preparedStatement.setString(1, entry.getKey());
                preparedStatement.setString(2, entry.getValue());
                preparedStatement.addBatch();
            }
        });
        assertEquals(results.length, 3);
        for (int result : results) {
            assertEquals(result, 1);
        }
    }

    @Test(expectedExceptions = DataAccessException.class)
    public void testExecuteBatchUpdateWithResultsWithErrorQuery() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        int[] results = jdbcTemplate.executeBatchUpdateWithResults(ERROR_QUERY, null);
    }

    @Test(expectedExceptions = DataAccessException.class)
    public void testExecuteBatchUpdateWithResultsWithErrorConnection() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(mockedDataSource);
        when(mockedDataSource.getConnection()).thenThrow(new SQLException());
        HashMap<String, String> testMap = new HashMap<>();
        testMap.put("testName1", "testDes1");
        jdbcTemplate.executeBatchUpdateWithResults(INSERT_QUERY, null);
    }

    @Test(expectedExceptions = DataAccessException.class)
    public void testExecuteBatchUpdateWithResultsWithDoInternalError() throws Exception {

        HashMap<Integer, String> testMap = new HashMap<>();
        testMap.put(1, "testDes1");
        testMap.put(2, "testDes2");

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        jdbcTemplate.executeBatchUpdateWithResults(INSERT_WITH_EXEC_ERROR_QUERY, preparedStatement -> {
            for (Map.Entry<Integer, String> entry : testMap.entrySet()) {
                preparedStatement.setInt(1, entry.getKey());
                preparedStatement.setString(2, entry.getValue());
            }
        });
    }

    @Test(priority = 2)
    public void testGetDriverName() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        assertEquals(jdbcTemplate.getDriverName(), "H2 JDBC Driver");
    }

    @Test(priority = 1, expectedExceptions = DataAccessException.class)
    public void testGetDriverNameWithErrorConnection() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(mockedDataSource);
        when(mockedDataSource.getConnection()).thenThrow(new SQLException());
        jdbcTemplate.getDriverName();
    }

    @Test(priority = 3)
    public void testGetDriverNameAlreadySet() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        assertNotNull(jdbcTemplate.getDriverName());
    }

    @Test(dataProvider = "exceptionLevelProvider", expectedExceptions = DataAccessException.class)
    public void testGetDriverNameWithException(int exceptionLevel) throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(mockedDataSource);
        Connection connection = Mockito.spy(basicDataSource.getConnection());
        when(mockedDataSource.getConnection()).thenReturn(connection);
        if (exceptionLevel == 1) {
            when(connection.getMetaData()).thenThrow(new SQLException());
        } else if (exceptionLevel == 2) {
            when(connection.getMetaData().getDriverName()).thenThrow(new SQLException());
        }
        jdbcTemplate.getDriverName();
    }

    @Test
    public void testGetDatabaseProductName() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(basicDataSource);
        assertEquals(jdbcTemplate.getDatabaseProductName(), "H2");
    }

    @Test(expectedExceptions = DataAccessException.class)
    public void testGetDatabaseProductNameWithErrorConnection() throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(mockedDataSource);
        when(mockedDataSource.getConnection()).thenThrow(new SQLException());
        jdbcTemplate.getDatabaseProductName();
    }

    @Test(dataProvider = "exceptionLevelProvider", expectedExceptions = DataAccessException.class)
    public void testGetDatabaseProductNameWithException(int exceptionLevel) throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(mockedDataSource);
        Connection connection = Mockito.spy(basicDataSource.getConnection());
        when(mockedDataSource.getConnection()).thenReturn(connection);
        if (exceptionLevel == 1) {
            when(connection.getMetaData()).thenThrow(new SQLException());
        } else if (exceptionLevel == 2) {
            when(connection.getMetaData().getDatabaseProductName()).thenThrow(new SQLException());
        }
        jdbcTemplate.getDatabaseProductName();
    }
}
