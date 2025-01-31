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

import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.wso2.carbon.database.utils.jdbc.exceptions.TransactionException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;

/**
 * This class contains the test cases for the JdbcTemplate with transaction support.
 */
@PrepareForTest({TransactionManager.class})
public class JdbcTemplateTransactionTest extends PowerMockTestCase {

    private static final String INSERT_QUERY = "INSERT INTO PURPOSE(NAME,DESCRIPTION) VALUES(?,?)";
    private static final String SELECT_QUERY = "SELECT * FROM PURPOSE;";
    private static final String SELECT_QUERY_WITH_FILTER = "SELECT * FROM PURPOSE WHERE NAME=?";
    private static final String SELECT_QUERY_WITHOUT_FILTER = "SELECT * FROM PURPOSE WHERE NAME='testName1'";
    private static final String ERROR_QUERY = "SELECT";
    private static final String UPDATE_QUERY_WITH_FILTER = "UPDATE PURPOSE SET DESCRIPTION=? WHERE NAME=?";
    private static final String UPDATE_QUERY_WITH_ERROR = "UPDATE PURPOSE SET ID=? WHERE NAME=?";
    private static final String UPDATE_WITH_STRING = "UPDATE PURPOSE SET DESCRIPTION=" +
            "'update description with string' WHERE NAME='testName1'";
    private static final String UPDATE_WITH_STRING_ERROR = "UPDATE PURPOSE SET ID=1 WHERE NAME='testName1'";

    @Mock
    private DataSource mockedDataSource;
    private DataSource dataSource;

    private static PurposeDummy mapRow(ResultSet resultSet, int rowNumber) throws SQLException {

        PurposeDummy purpose = new PurposeDummy();
        purpose.setId(resultSet.getInt(1));
        purpose.setName(resultSet.getString(2));
        purpose.setDescription(resultSet.getString(3));
        return purpose;
    }

    @BeforeMethod
    public void setUp() throws Exception {

        JdbcTemplateTestUtils testUtils = new JdbcTemplateTestUtils();
        testUtils.initH2Database("testDB", JdbcTemplateTestUtils.getFilePath("h2.sql"));
        dataSource = testUtils.getDataSource();
    }

    /**
     * This test case shows how to use the JdbcTransactionTemplate's methods.
     */
    @Test(priority = 1)
    public void testExecuteInsertWithMultipleExecutions() {

        int[] res = new int[0];
        JdbcTemplate transactionTemplate = new JdbcTemplate(dataSource);
        try {
            res = transactionTemplate.withTransaction((template) -> {

                int[] results = new int[4];
                String query = "INSERT INTO PURPOSE(NAME,DESCRIPTION) VALUES(?,?)";
                results[0] = template.executeInsert(query, preparedStatement -> {
                    preparedStatement.setString(1, "Dummy purpose name 001");
                    preparedStatement.setString(2, "Dummy purpose des 001");
                }, null, true);

                insertPurpose(results);

                query = "INSERT INTO DATA_CONTROLLER(NAME) VALUES(?)";
                results[2] = template.executeInsert(query, preparedStatement ->
                        preparedStatement.setString(1, "Dummy DC name 001"), null, true);

                results[3] = template.executeInsert(query, preparedStatement ->
                        preparedStatement.setString(1, "Dummy DC name 002"), null, true);
                return results;
            });
        } catch (TransactionException e) {
            //Can throw any exception from here to the upper layer or log the error.
        }
        assertEquals(res[0], 1);
        assertEquals(res[2], 1);
    }

    private void insertPurpose(int[] results) throws TransactionException {

        JdbcTemplate transactionTemplateInternal = new JdbcTemplate(dataSource);
        String finalQuery = "INSERT INTO PURPOSE(NAME,DESCRIPTION) VALUES(?,?)";
        transactionTemplateInternal.withTransaction((templateInternal) -> {
            results[1] = templateInternal.executeInsert(finalQuery, preparedStatement -> {
                preparedStatement.setString(1, "Dummy purpose name 001");
                preparedStatement.setString(2, "Dummy purpose des 002");
            }, null, true);
            return null;
        });
    }

    @Test(priority = 2, expectedExceptions = TransactionException.class)
    public void testTransactionSupportWithMultipleExecutionsAndExceptions() throws Exception {

        JdbcTemplate transactionTemplate = new JdbcTemplate(dataSource);
        transactionTemplate.withTransaction((template) -> {

            int[] results = new int[4];
            String query = "INSERT INTO PURPOSE(NAME,DESCRIPTION) VALUES(?,?)";
            results[0] = template.executeInsert(query, preparedStatement -> {
                preparedStatement.setString(1, "Dummy purpose name 001");
                preparedStatement.setString(2, "Dummy purpose des 001");
            }, null, true);

            executeInsertWithErrorQuery(results);
            return results;

        });
    }

    private void executeInsertWithErrorQuery(int[] results) throws Exception {

        JdbcTemplate transactionTemplate = new JdbcTemplate(dataSource);
        String finalQuery = "INTO PURPOSE(NAME,DESCRIPTION) VALUES(?,?)";
        transactionTemplate.withTransaction((template) -> {
            results[1] = template.executeInsert(finalQuery, preparedStatement -> {
                preparedStatement.setString(1, "Dummy purpose name 001");
                preparedStatement.setString(2, "Dummy purpose des 002");
            }, null, true);
            return null;
        });
    }

    @Test(priority = 3)
    public void testTransactionSupportWithSingleQuery() throws Exception {

        JdbcTemplate transactionTemplate = new JdbcTemplate(dataSource);
        int id;

        id = transactionTemplate.withTransaction((template) -> {
            int pId;
            String query = "INSERT INTO PURPOSE(NAME,DESCRIPTION) VALUES(?,?)";
            pId = template.executeInsert(query, preparedStatement -> {
                preparedStatement.setString(1, "Dummy purpose name 001");
                preparedStatement.setString(2, "Dummy description");
            }, null, true);
            return pId;

        });
        assertNotEquals(id, 0);
    }

    @Test(priority = 4, expectedExceptions = TransactionException.class)
    public void testTransactionSupportWithException() throws Exception {

        JdbcTemplate transactionTemplate = new JdbcTemplate(dataSource);

        transactionTemplate.withTransaction((template) -> {

            int id;
            String query = "I INTO PURPOSE(NAME,DESCRIPTION) VALUES(?,?)";
            id = template.executeInsert(query, preparedStatement -> {
                preparedStatement.setString(1, "Dummy purpose name 001");
                preparedStatement.setString(2, "Dummy description");
            }, null, true);
            return id;
        });
    }

    @Test(priority = 5, expectedExceptions = TransactionException.class)
    public void testTransactionSupportWithNullConnection() throws Exception {

        JdbcTemplate transactionTemplate = new JdbcTemplate(mockedDataSource);
        when(mockedDataSource.getConnection()).thenThrow(new SQLException());

        transactionTemplate.withTransaction((template) -> {

            int id;
            String query = "INSERT INTO PURPOSE(NAME,DESCRIPTION) VALUES(?,?)";
            id = template.executeInsert(query, preparedStatement -> {
                preparedStatement.setString(1, "Dummy purpose name 001");
                preparedStatement.setString(2, "Dummy description");
            }, null, true);
            return id;
        });
    }

    @DataProvider(name = "exceptionLevelProvider")
    public static Object[][] provideExceptionLevels() {

        return new Object[][]{
                {1},
                {2},
                {3}
        };
    }

    @Test(dataProvider = "exceptionLevelProvider", expectedExceptions = TransactionException.class)
    public void testTransactionSupportWithTransactionError(int exceptionLevel) throws Exception {

        JdbcTemplate jdbcTemplate = new JdbcTemplate(mockedDataSource);
        Connection connection = Mockito.spy(dataSource.getConnection());
        when(mockedDataSource.getConnection()).thenReturn(connection);
        if (exceptionLevel == 1) {
            doThrow(new SQLException()).when(connection).rollback();
        } else if (exceptionLevel == 2) {
            doThrow(new SQLException()).when(connection).close();
        } else if (exceptionLevel == 3) {
            doThrow(new SQLException()).when(connection).setAutoCommit(true);
        }

        jdbcTemplate.withTransaction((template) -> {

            int id;
            String query = "INERT INTO PURPOSE(NAME,DESCRIPTION) VALUES(?,?)";
            id = template.executeInsert(query, preparedStatement -> {
                preparedStatement.setString(1, "Dummy purpose name 001");
                preparedStatement.setString(2, "Dummy description");
            }, null, true);
            return id;
        });
    }

    @Test(priority = 6)
    public void testExecuteInsertWithFetchInsertedIdFalse() throws Exception {

        JdbcTemplate transactionTemplate = new JdbcTemplate(dataSource);
        int id = transactionTemplate.withTransaction((template) -> {

            int pId;
            String query = "INSERT INTO PURPOSE(NAME,DESCRIPTION) VALUES(?,?)";
            pId = template.executeInsert(query, preparedStatement -> {
                preparedStatement.setString(1, "Dummy purpose name 001");
                preparedStatement.setString(2, "Dummy description");
            }, null, false);
            return pId;
        });
        assertEquals(id, 0);
    }

    @Test(priority = 7, expectedExceptions = TransactionException.class)
    public void testExecuteInsertWithDoInternalUpdateError() throws Exception {

        JdbcTemplate transactionTemplate = new JdbcTemplate(dataSource);
        transactionTemplate.withTransaction((template) -> {

            template.executeInsert(INSERT_QUERY, (preparedStatement -> {
                preparedStatement.setString(1, "Test name 1");
                preparedStatement.setString(2, null);
            }), null, false);
            return null;
        });
    }

    @Test(priority = 8)
    public void testExecuteBatchInsert() throws Exception {

        JdbcTemplate transactionTemplate = new JdbcTemplate(dataSource);
        transactionTemplate.withTransaction((template) -> {

            HashMap<String, String> testMap = new HashMap<>();
            testMap.put("testName1", "testDes1");
            testMap.put("testName2", "testDes2");
            testMap.put("testName3", "testDes3");

            template.executeBatchInsert(INSERT_QUERY, preparedStatement -> {
                for (Map.Entry<String, String> entry : testMap.entrySet()) {
                    preparedStatement.setString(1, entry.getKey());
                    preparedStatement.setString(2, entry.getValue());
                    preparedStatement.addBatch();
                }
            }, "testBean");
            return null;
        });
    }

    @Test(priority = 9, expectedExceptions = TransactionException.class)
    public void testExecuteBatchInsertWithErrorQuery() throws Exception {

        JdbcTemplate transactionTemplate = new JdbcTemplate(dataSource);
        transactionTemplate.withTransaction((template) -> {

            HashMap<String, String> testMap = new HashMap<>();
            testMap.put("testName1", "testDes1");
            template.executeBatchInsert(ERROR_QUERY, null, null);
            return null;
        });
    }

    @Test(priority = 10, expectedExceptions = TransactionException.class)
    public void testExecuteBatchInsertWithDoInternalError() throws Exception {

        JdbcTemplate transactionTemplate = new JdbcTemplate(dataSource);
        transactionTemplate.withTransaction((template) -> {

            HashMap<String, String> testMap = new HashMap<>();
            testMap.put("testName1", "testDes1");
            template.executeBatchInsert(INSERT_QUERY, preparedStatement -> {
                for (Map.Entry<String, String> entry : testMap.entrySet()) {
                    preparedStatement.setString(1, entry.getKey());
                    preparedStatement.setString(2, null);
                    preparedStatement.addBatch();
                }
            }, "testBean");
            return null;
        });
    }

    @Test(priority = 11, dependsOnMethods = "testExecuteBatchInsert")
    public void testExecuteQuery() throws Exception {

        JdbcTemplate transactionTemplate = new JdbcTemplate(dataSource);
        List<PurposeDummy> purposeList = transactionTemplate.withTransaction((template) ->
                template.executeQuery(SELECT_QUERY, JdbcTemplateTransactionTest::mapRow));
        assertEquals(purposeList.get(0).getName(), "Dummy purpose name 001");
    }

    @Test(priority = 12, expectedExceptions = TransactionException.class)
    public void testExecuteQueryWithErrorQuery() throws Exception {

        JdbcTemplate transactionTemplate = new JdbcTemplate(dataSource);
        transactionTemplate.withTransaction((template) -> {

            template.executeQuery(ERROR_QUERY, JdbcTemplateTransactionTest::mapRow);
            return null;
        });
    }

    @Test(priority = 13, dependsOnMethods = "testExecuteBatchInsert")
    public void testExecuteQueryWithFilter() throws Exception {

        JdbcTemplate transactionTemplate = new JdbcTemplate(dataSource);
        List<PurposeDummy> purposeList = transactionTemplate.withTransaction((template) ->
                template.executeQuery(SELECT_QUERY_WITH_FILTER,
                        JdbcTemplateTransactionTest::mapRow, preparedStatement ->
                                preparedStatement.setString(1, "testName1")));
        assertEquals(purposeList.get(0).getDescription(), "testDes1");
    }

    @Test(priority = 14, dependsOnMethods = "testExecuteBatchInsert")
    public void testFetchSingleRecord() throws Exception {

        JdbcTemplate transactionTemplate = new JdbcTemplate(dataSource);
        PurposeDummy assertionPurpose = transactionTemplate.withTransaction((template) ->
                template.fetchSingleRecord(SELECT_QUERY_WITH_FILTER,
                        JdbcTemplateTransactionTest::mapRow, preparedStatement ->
                                preparedStatement.setString(1, "testName1")));
        assertEquals(assertionPurpose.getDescription(), "testDes1");
    }

    @Test(priority = 15, dependsOnMethods = "testExecuteBatchInsert")
    public void testFetchSingleRecordNoFilter() throws Exception {

        JdbcTemplate transactionTemplate = new JdbcTemplate(dataSource);
        PurposeDummy assertionPurpose = transactionTemplate.withTransaction((template) ->
                template.fetchSingleRecord(SELECT_QUERY_WITHOUT_FILTER,
                        JdbcTemplateTransactionTest::mapRow, null));
        assertEquals(assertionPurpose.getDescription(), "testDes1");
    }

    @Test(priority = 16, expectedExceptions = TransactionException.class)
    public void testFetchSingleRecordWithErrorQuery() throws Exception {

        JdbcTemplate transactionTemplate = new JdbcTemplate(dataSource);
        transactionTemplate.withTransaction((template) -> {
            template.fetchSingleRecord(ERROR_QUERY, JdbcTemplateTransactionTest::mapRow, null);
            return null;
        });
    }

    @Test(priority = 17, expectedExceptions = TransactionException.class, dependsOnMethods = "testExecuteBatchInsert")
    public void testFetchSingleRecordWithMultipleResults() throws Exception {

        JdbcTemplate transactionTemplate = new JdbcTemplate(dataSource);
        transactionTemplate.withTransaction((template) -> {
            template.fetchSingleRecord(SELECT_QUERY, JdbcTemplateTransactionTest::mapRow, null);
            return null;
        });
    }

    @Test(priority = 18)
    public void testFetchSingleRecordNoResult() throws Exception {

        JdbcTemplate transactionTemplate = new JdbcTemplate(dataSource);
        PurposeDummy assertionPurpose = transactionTemplate.withTransaction((template) ->
                template.fetchSingleRecord(SELECT_QUERY_WITH_FILTER, JdbcTemplateTransactionTest::mapRow,
                        preparedStatement -> preparedStatement.setString(1, "dummy")));
        assertNull(assertionPurpose);
    }

    @Test(priority = 19, dependsOnMethods = "testExecuteBatchInsert")
    public void testExecuteUpdate() throws Exception {

        JdbcTemplate transactionTemplate = new JdbcTemplate(dataSource);
        transactionTemplate.withTransaction((template) -> {
            template.executeUpdate(UPDATE_QUERY_WITH_FILTER, preparedStatement -> {
                preparedStatement.setString(1, "updated description 001");
                preparedStatement.setString(2, "testName1");
            });
            return null;
        });
    }

    @Test(priority = 20, expectedExceptions = TransactionException.class)
    public void testExecuteUpdateWithQueryError() throws Exception {

        JdbcTemplate transactionTemplate = new JdbcTemplate(dataSource);
        transactionTemplate.withTransaction((template) -> {
            template.executeUpdate(ERROR_QUERY, null);
            return null;
        });
    }

    @Test(priority = 21, expectedExceptions = TransactionException.class, dependsOnMethods = "testExecuteBatchInsert")
    public void testExecuteUpdateWithFilterAndError() throws Exception {

        JdbcTemplate transactionTemplate = new JdbcTemplate(dataSource);
        transactionTemplate.withTransaction((template) -> {
            template.executeUpdate(UPDATE_QUERY_WITH_ERROR, preparedStatement -> {
                preparedStatement.setInt(1, 0);
                preparedStatement.setString(3, "testName1");
            });
            return null;
        });
    }

    @Test(priority = 22, expectedExceptions = TransactionException.class)
    public void testExecuteUpdateWithoutFilterWithError() throws Exception {

        JdbcTemplate transactionTemplate = new JdbcTemplate(dataSource);
        transactionTemplate.withTransaction((template) -> {
            template.executeUpdate(UPDATE_QUERY_WITH_ERROR, null);
            return null;
        });
    }

    @Test(priority = 23, dependsOnMethods = "testExecuteBatchInsert")
    public void testExecuteUpdateString() throws Exception {

        JdbcTemplate transactionTemplate = new JdbcTemplate(dataSource);
        int id = transactionTemplate.withTransaction((template) -> template.executeUpdate(UPDATE_WITH_STRING));
        assertEquals(id, 1);
    }

    @Test(priority = 24, expectedExceptions = TransactionException.class)
    public void testExecuteUpdateStringWithErrorQuery() throws Exception {

        JdbcTemplate transactionTemplate = new JdbcTemplate(dataSource);
        transactionTemplate.withTransaction((template) -> {
            template.executeUpdate(ERROR_QUERY);
            return null;
        });
    }

    @Test(priority = 25, dependsOnMethods = "testExecuteBatchInsert", expectedExceptions = TransactionException.class)
    public void testExecuteUpdateStringWithDoInternalError() throws Exception {

        JdbcTemplate transactionTemplate = new JdbcTemplate(dataSource);
        transactionTemplate.withTransaction((template) -> {
            template.executeUpdate(UPDATE_WITH_STRING_ERROR);
            return null;
        });
    }

    @Test(priority = 26, dependsOnMethods = "testExecuteBatchInsert")
    public void testExecuteUpdateWithAffectedRows() throws Exception {

        JdbcTemplate transactionTemplate = new JdbcTemplate(dataSource);
        transactionTemplate.withTransaction((template) -> {
            int numberOfAffectedRows = template.executeUpdateWithAffectedRows(UPDATE_QUERY_WITH_FILTER,
                    preparedStatement -> {
                        preparedStatement.setString(1, "updated description 001");
                        preparedStatement.setString(2, "testName1");
                    });
            assertNotEquals(numberOfAffectedRows, 0);
            return null;
        });
    }
}
