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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;
import org.wso2.carbon.database.utils.jdbc.exceptions.DataAccessException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class created to have Template which contains the methods for the database transactions is different from
 * Template class as it supports the use of named prepared statement.
 *
 * @param <T> defines the input data type.
 */
public class NamedTemplate<T> {

    private static final Logger log = LoggerFactory.getLogger(NamedTemplate.class);

    /**
     * @param query            The SQL for insert/update.
     * @param namedQueryFilter Query filter to named prepared statement parameter binding.
     * @param bean             the Domain object to be inserted/updated.
     * @param fetchInsertedId  the boolean value to get the inserted id.
     * @param <T>              return type of the object.
     */
    public <T> int executeInsert(String query, NamedQueryFilter namedQueryFilter, T bean, boolean fetchInsertedId)
            throws DataAccessException {

        Connection connection = TransactionManager.getTransactionEntry().getConnection();
        int resultId;
        try {
            if (fetchInsertedId) {
                try (NamedPreparedStatement namedPreparedStatement = new NamedPreparedStatement(connection, query)) {
                    doInternalUpdate(namedQueryFilter, namedPreparedStatement);
                    logDebug("Mapping generated key (Auto Increment ID) to the object");
                    try (ResultSet generatedKeys = namedPreparedStatement.getGeneratedKeys()) {
                        if (generatedKeys.next()) {
                            resultId = generatedKeys.getInt(1);
                            logDebug("Newly inserted ID (Auto Increment ID) is {} for the bean {} ",
                                    resultId, bean);
                        } else {
                            throw new SQLException(JdbcConstants.ErrorCodes.ERROR_CODE_AUTO_GENERATED_ID_FAILURE.
                                    getErrorMessage());
                        }
                    }
                }
                return resultId;
            } else {
                try (NamedPreparedStatement namedPreparedStatement = new NamedPreparedStatement(connection, query)) {
                    doInternalUpdate(namedQueryFilter, namedPreparedStatement);
                }
            }
        } catch (SQLException e) {
            logDebug("Error in performing database insert: {} with parameters {}", query, namedQueryFilter);
            throw new DataAccessException(JdbcConstants.ErrorCodes.ERROR_CODE_DATABASE_QUERY_PERFORMING.
                    getErrorMessage() + " " + query, JdbcConstants.ErrorCodes.ERROR_CODE_DATABASE_QUERY_PERFORMING.
                    getErrorCode(), e);
        }
        return 0;
    }

    /**
     * Executes the jdbc insert/update query.
     *
     * @param query            The SQL for insert/update.
     * @param namedQueryFilter Query filter to named prepared statement parameter binding.
     * @param bean             the Domain object to be inserted/updated.
     */
    public <T extends Object> int executeBatchInsert(String query, NamedQueryFilter namedQueryFilter, T bean)
            throws DataAccessException {

        Connection connection = TransactionManager.getTransactionEntry().getConnection();
        try (NamedPreparedStatement namedPreparedStatement = new NamedPreparedStatement(connection, query)) {
            doInternalBatchUpdate(namedQueryFilter, namedPreparedStatement);
        } catch (SQLException e) {
            logDebug("Error in performing database insert: {} with parameters {}", query, namedQueryFilter);
            throw new DataAccessException(JdbcConstants.ErrorCodes.ERROR_CODE_DATABASE_QUERY_PERFORMING_ERROR
                    .getErrorMessage() + " " + query, e);
        }
        return 0;
    }

    /**
     * Executes a query on JDBC and return the result as a list of domain objects.
     *
     * @param query     the SQL query with the parameter placeholders.
     * @param rowMapper Row mapper functional interface.
     * @return List of domain objects of required type.
     * @see #executeQuery(String, RowMapper, NamedQueryFilter)
     */
    public <T> List<T> executeQuery(String query, RowMapper<T> rowMapper) throws
            DataAccessException {

        return executeQuery(query, rowMapper, null);
    }

    /**
     * Executes a query on JDBC and return the result as a list of domain objects.
     *
     * @param query            the SQL query with the parameter placeholders.
     * @param rowMapper        Row mapper functional interface.
     * @param namedQueryFilter parameters for the SQL query parameter replacement.
     * @return List of domain objects of required type.
     */
    public <T> List<T> executeQuery(String query, RowMapper<T> rowMapper, NamedQueryFilter namedQueryFilter)
            throws DataAccessException {

        List<T> result = new ArrayList<>();
        Connection connection = TransactionManager.getTransactionEntry().getConnection();
        try {
            try (NamedPreparedStatement namedPreparedStatement = new NamedPreparedStatement(connection, query)) {
                if (namedQueryFilter != null) {
                    namedQueryFilter.filter(namedPreparedStatement);
                }
                try (ResultSet resultSet = namedPreparedStatement.executeQuery()) {
                    int i = 0;
                    while (resultSet.next()) {
                        T row = rowMapper.mapRow(resultSet, i);
                        result.add(row);
                        i++;
                    }
                }
            }
        } catch (SQLException e) {
            logDebug("There has been an error performing the database query. The query is {}, and the Parameters" +
                    " are {}", e, query, namedQueryFilter);
            throw new DataAccessException(JdbcConstants.ErrorCodes.ERROR_CODE_DATABASE_QUERY_PERFORMING_ERROR
                    .getErrorMessage() + " " + query, e);
        }
        return result;
    }

    /**
     * Executes a query on JDBC and return the result as a domain object.
     *
     * @param query            the SQL query with the parameter placeholders.
     * @param rowMapper        Row mapper functional interface.
     * @param namedQueryFilter parameters for the SQL query parameter replacement.
     * @return domain object of required type.
     */
    public <T> T fetchSingleRecord(String query, RowMapper<T> rowMapper, NamedQueryFilter namedQueryFilter)
            throws DataAccessException {

        T result = null;
        Connection connection = TransactionManager.getTransactionEntry().getConnection();
        try {
            try (NamedPreparedStatement namedPreparedStatement = new NamedPreparedStatement(connection, query)) {
                if (namedQueryFilter != null) {
                    namedQueryFilter.filter(namedPreparedStatement);
                }
                try (ResultSet resultSet = namedPreparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        result = rowMapper.mapRow(resultSet, 0);
                    }
                    if (!resultSet.isClosed() && resultSet.next()) {
                        logDebug("There are more records than one found for query: {} for the parameters {}", query,
                                namedQueryFilter);
                        throw new DataAccessException(JdbcConstants.ErrorCodes.ERROR_CODE_MORE_RECORDS_IN_SINGLE_FETCH.
                                getErrorMessage() + " " + query);
                    }
                }
            }
        } catch (SQLException e) {
            logDebug("There has been an error performing the database query. The query is {}, and the " +
                    "parameters are {}", e, query, rowMapper, namedQueryFilter);
            throw new DataAccessException(JdbcConstants.ErrorCodes.ERROR_CODE_DATABASE_QUERY_PERFORMING_ERROR
                    .getErrorMessage() + " " + query, e);
        }
        return result;
    }

    /**
     * Executes the jdbc update query and returns nothing.
     *
     * @param query            SQL query with the parameter placeholders.
     * @param namedQueryFilter parameters for the SQL query parameter replacement.
     */
    public void executeUpdate(String query, NamedQueryFilter namedQueryFilter) throws DataAccessException {

        Connection connection = TransactionManager.getTransactionEntry().getConnection();
        try {
            try (NamedPreparedStatement namedPreparedStatement = new NamedPreparedStatement(connection, query)) {
                if (namedQueryFilter != null) {
                    namedQueryFilter.filter(namedPreparedStatement);
                }
                namedPreparedStatement.executeUpdate();
            }

        } catch (SQLException e) {
            logDebug("Error in performing database update: {} with parameters {}", query, namedQueryFilter);
            throw new DataAccessException(JdbcConstants.ErrorCodes.ERROR_CODE_DATABASE_QUERY_PERFORMING_ERROR
                    .getErrorMessage() + " " + query, e);
        }
    }

    /**
     * Executes the jdbc update query and returns the result as updated id integer.
     *
     * @param query the SQL query with the parameter placeholders.
     * @return the updated id.
     */
    public int executeUpdate(String query) throws DataAccessException {

        Connection connection = TransactionManager.getTransactionEntry().getConnection();
        try (NamedPreparedStatement namedPreparedStatement = new NamedPreparedStatement(connection, query)) {
            return doInternalUpdate(null, namedPreparedStatement);
        } catch (SQLException e) {
            logDebug("Error in performing database update: {}", query);
            throw new DataAccessException(JdbcConstants.ErrorCodes.ERROR_CODE_DATABASE_QUERY_PERFORMING_ERROR
                    .getErrorMessage() + " " + query, e);
        }
    }

    private int doInternalUpdate(NamedQueryFilter namedQueryFilter, NamedPreparedStatement namedPreparedStatement)
            throws SQLException, DataAccessException {

        if (namedQueryFilter != null) {
            namedQueryFilter.filter(namedPreparedStatement);
        }
        return namedPreparedStatement.executeUpdate();
    }

    private <T extends Object> void doInternalBatchUpdate(NamedQueryFilter namedQueryFilter, NamedPreparedStatement
            namedPreparedStatement) throws SQLException, DataAccessException {

        if (namedQueryFilter != null) {
            namedQueryFilter.filter(namedPreparedStatement);
        }
        namedPreparedStatement.executeBatch();
    }

    private void logDebug(String s, Object... params) {

        logDebug(s, null, params);
    }

    private void logDebug(String s, Exception e, Object... params) {

        if (log.isDebugEnabled()) {
            log.debug(MessageFormatter.arrayFormat(s, params).getMessage(), e);
        }
    }
}
