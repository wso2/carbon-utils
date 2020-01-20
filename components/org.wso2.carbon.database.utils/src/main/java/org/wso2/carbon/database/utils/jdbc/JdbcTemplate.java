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

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;
import org.wso2.carbon.database.utils.jdbc.exceptions.DataAccessException;
import org.wso2.carbon.database.utils.jdbc.exceptions.TransactionException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

/**
 * A utility class to call JDBC with lambda expressions. This class simplifies the usage of JDBC and helps to avoid
 * common errors.
 * Using this class we can execute insert, select, update and delete queries by providing necessary parameters to the
 * methods. Transaction support is also available with this implementation for the query executions. All the SQL
 * operations performed by this class are logged at the debug level.
 */
public class JdbcTemplate {

    private static final Logger log = LoggerFactory.getLogger(JdbcTemplate.class);
    private static final String ID = "ID";
    private String driverName;
    private String productName;
    private DataSource dataSource;

    public JdbcTemplate(DataSource dataSource) {

        this.dataSource = dataSource;
    }

    /**
     * Provides the transaction support for the JDBC executions of the template.
     *
     * @param callable the SQL query execution method call.
     * @return result set of domain objects of required type.
     */
    public <T extends Object, E extends Exception> T withTransaction(ExecuteCallable<T> callable) throws
            TransactionException, E {

        TransactionEntry transactionEntry = TransactionManager.getTransactionEntry();
        if (transactionEntry.getTransactionDepth() == 0) {

            try {
                Connection connection = dataSource.getConnection();
                transactionEntry.setAutoCommitState(connection.getAutoCommit());
                connection.setAutoCommit(false);
                transactionEntry.setConnection(connection);
            } catch (SQLException e) {
                throw new TransactionException(JdbcConstants.ErrorCodes.ERROR_CODE_GETTING_CONNECTION_FAILURE.
                        getErrorMessage(), JdbcConstants.ErrorCodes.ERROR_CODE_GETTING_CONNECTION_FAILURE.
                        getErrorCode(), e);
            }
        }
        transactionEntry.incrementTransactionDepth();
        TransactionManager.setTransactionEntry(transactionEntry);
        try {
            T result = callable.get(new Template<>());
            transactionEntry.decrementTransactionDepth();
            if (transactionEntry.getTransactionDepth() == 0) {
                transactionEntry.getConnection().commit();
            }
            return result;
        } catch (Throwable t) {
            //We catch any exception and rollback the transaction.
            logDebug("Could not commit the transaction.", t);
            transactionEntry.decrementTransactionDepth();
            if (transactionEntry.getTransactionDepth() == 0) {
                try {
                    transactionEntry.getConnection().rollback();
                } catch (SQLException e) {
                    logDebug("Could not rollback the transaction.", e);
                    throw new TransactionException(JdbcConstants.ErrorCodes.ERROR_CODE_ROLLBACK_TRANSACTION_FAILURE.
                            getErrorMessage(), JdbcConstants.ErrorCodes.ERROR_CODE_ROLLBACK_TRANSACTION_FAILURE.
                            getErrorCode(), e);
                }
            }
            throw new TransactionException(JdbcConstants.ErrorCodes.ERROR_CODE_COMMIT_TRANSACTION_FAILURE.
                    getErrorMessage(), JdbcConstants.ErrorCodes.ERROR_CODE_COMMIT_TRANSACTION_FAILURE.
                    getErrorCode(), t);
        } finally {
            if (transactionEntry.getTransactionDepth() == 0) {
                try {
                    transactionEntry.getConnection().setAutoCommit(transactionEntry.getAutoCommitState());
                    transactionEntry.getConnection().close();
                    transactionEntry.setConnection(null);
                } catch (SQLException e) {
                    logDebug("Could not close the transaction.", e);
                    throw new TransactionException(JdbcConstants.ErrorCodes.ERROR_CODE_CLOSE_CONNECTION_FAILURE.
                            getErrorMessage(), JdbcConstants.ErrorCodes.ERROR_CODE_CLOSE_CONNECTION_FAILURE.
                            getErrorCode(), e);
                }
                TransactionManager.exitTransaction();
            }
        }
    }

    /**
     * Executes a query on JDBC and return the result as a list of domain objects.
     *
     * @param query     the SQL query with the parameter placeholders.
     * @param rowMapper Row mapper functional interface.
     * @return List of domain objects of required type.
     * @see #executeQuery(String, RowMapper, QueryFilter)
     */
    public <T> List<T> executeQuery(String query, RowMapper<T> rowMapper) throws
            DataAccessException {

        return executeQuery(query, rowMapper, null);
    }

    /**
     * Executes a query on JDBC and return the result as a list of domain objects.
     *
     * @param query       the SQL query with the parameter placeholders.
     * @param rowMapper   Row mapper functional interface.
     * @param queryFilter parameters for the SQL query parameter replacement.
     * @return List of domain objects of required type.
     */
    public <T> List<T> executeQuery(String query, RowMapper<T> rowMapper, QueryFilter queryFilter)
            throws DataAccessException {

        List<T> result = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            if (queryFilter != null) {
                queryFilter.filter(preparedStatement);
            }
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                int i = 0;
                while (resultSet.next()) {
                    T row = rowMapper.mapRow(resultSet, i);
                    result.add(row);
                    i++;
                }
            }
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        } catch (SQLException e) {
            logDebug("There has been an error performing the database query. The query is {}, and the Parameters" +
                    " are {}", e, query, queryFilter);
            throw new DataAccessException(JdbcConstants.ErrorCodes.ERROR_CODE_DATABASE_QUERY_PERFORMING_ERROR
                    .getErrorMessage() + query, e);
        }
        return result;
    }

    /**
     * Executes a query on JDBC and return the result as a domain object.
     *
     * @param query       the SQL query with the parameter placeholders.
     * @param rowMapper   Row mapper functional interface.
     * @param queryFilter parameters for the SQL query parameter replacement.
     * @return domain object of required type.
     */
    public <T> T fetchSingleRecord(String query, RowMapper<T> rowMapper, QueryFilter queryFilter)
            throws DataAccessException {

        T result = null;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            if (queryFilter != null) {
                queryFilter.filter(preparedStatement);
            }
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    result = rowMapper.mapRow(resultSet, 0);
                }
                if (!resultSet.isClosed() && resultSet.next()) {
                    logDebug("There are more records than one found for query: {} for the parameters {}", query,
                            queryFilter);
                    throw new DataAccessException(JdbcConstants.ErrorCodes.ERROR_CODE_MORE_RECORDS_IN_SINGLE_FETCH.
                            getErrorMessage() + query);
                }
            }
        } catch (SQLException e) {
            logDebug("There has been an error performing the database query. The query is {}, and the parameters" +
                    " are {}", e, query, rowMapper, queryFilter);
            throw new DataAccessException(JdbcConstants.ErrorCodes.ERROR_CODE_DATABASE_QUERY_PERFORMING_ERROR
                    .getErrorMessage() + query, e);
        }
        return result;
    }

    /**
     * Executes the jdbc update query and returns nothing.
     *
     * @param query       SQL query with the parameter placeholders.
     * @param queryFilter parameters for the SQL query parameter replacement.
     */
    public void executeUpdate(String query, QueryFilter queryFilter) throws DataAccessException {

        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            if (queryFilter != null) {
                queryFilter.filter(preparedStatement);
            }
            preparedStatement.executeUpdate();
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        } catch (SQLException e) {
            logDebug("Error in performing database update: {} with parameters {}", query, queryFilter);
            throw new DataAccessException(JdbcConstants.ErrorCodes.ERROR_CODE_DATABASE_QUERY_PERFORMING_ERROR
                    .getErrorMessage() + query, e);
        }
    }

    /**
     * Executes the jdbc update query and returns the result as updated id integer.
     *
     * @param query the SQL query with the parameter placeholders.
     * @return the updated id.
     */
    public int executeUpdate(String query) throws DataAccessException {

        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            int id = doInternalUpdate(null, preparedStatement);
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
            return id;
        } catch (SQLException e) {
            logDebug("Error in performing database update: {}", query);
            throw new DataAccessException(JdbcConstants.ErrorCodes.ERROR_CODE_DATABASE_QUERY_PERFORMING_ERROR
                    .getErrorMessage() + query, e);
        }
    }

    /**
     * Executes the jdbc insert/update query.
     *
     * @param query       The SQL for insert/update.
     * @param queryFilter Query filter to prepared statement parameter binding.
     * @param bean        the Domain object to be inserted/updated.
     * @param <T>         return type of the object.
     */
    public <T> int executeInsert(String query, QueryFilter queryFilter, T bean, boolean fetchInsertedId)
            throws DataAccessException {

        return executeInsert(query, queryFilter, bean, fetchInsertedId, ID);
    }

    /**
     * Executes the jdbc insert/update query depends on provided auto generated column name.
     *
     * @param query                    The SQL for insert/update.
     * @param queryFilter              Query filter to prepared statement parameter binding.
     * @param bean                     The Domain object to be inserted/updated.
     * @param fetchInsertedId          Fetch inserted ID.
     * @param autoGenerateIdColumnName Name of auto generate ID column.
     * @param <T>                      Return type of the object.
     * @return
     * @throws DataAccessException
     */
    public <T> int executeInsert(String query, QueryFilter queryFilter, T bean, boolean fetchInsertedId,
                                 String autoGenerateIdColumnName)
            throws DataAccessException {

        try (Connection connection = dataSource.getConnection()) {
            int resultId;
            if (fetchInsertedId) {
                String dbProductName = connection.getMetaData().getDatabaseProductName();
                try (PreparedStatement preparedStatement = connection.prepareStatement(query, new String[]{
                        JdbcUtils.getConvertedAutoGeneratedColumnName(dbProductName, autoGenerateIdColumnName)})) {
                    doInternalUpdate(queryFilter, preparedStatement);
                    logDebug("Mapping generated key (Auto Increment ID) to the object");
                    try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
                        if (generatedKeys.next()) {
                            resultId = generatedKeys.getInt(1);
                            logDebug("Newly inserted {} (Auto Increment ID) is {} for the bean {} ",
                                    autoGenerateIdColumnName, resultId, bean);
                        } else {
                            throw new SQLException(JdbcConstants.ErrorCodes.ERROR_CODE_AUTO_GENERATED_ID_FAILURE.
                                    getErrorMessage());
                        }
                    }
                }
                if (!connection.getAutoCommit()) {
                    connection.commit();
                }
                return resultId;
            } else {
                try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                    doInternalUpdate(queryFilter, preparedStatement);
                }
                if (!connection.getAutoCommit()) {
                    connection.commit();
                }
            }
        } catch (SQLException e) {
            logDebug("Error in performing database insert: {} with parameters {}", query, queryFilter);
            throw new DataAccessException(JdbcConstants.ErrorCodes.ERROR_CODE_DATABASE_QUERY_PERFORMING.
                    getErrorMessage() + query, JdbcConstants.ErrorCodes.ERROR_CODE_DATABASE_QUERY_PERFORMING.
                    getErrorCode(), e);
        }
        return 0;
    }

    /**
     * Executes the jdbc insert/update query.
     *
     * @param query       The SQL for insert/update.
     * @param queryFilter Query filter to prepared statement parameter binding.
     * @param bean        the Domain object to be inserted/updated.
     */
    public <T extends Object> int executeBatchInsert(String query, QueryFilter queryFilter, T bean)
            throws DataAccessException {

        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            doInternalBatchUpdate(queryFilter, preparedStatement);
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        } catch (SQLException e) {
            logDebug("Error in performing database insert: {} with parameters {}", query, queryFilter);
            throw new DataAccessException(JdbcConstants.ErrorCodes.ERROR_CODE_DATABASE_QUERY_PERFORMING_ERROR
                    .getErrorMessage() + query, e);
        }
        return 0;
    }

    /**
     * Executes the jdbc batch delete query.
     *
     * @param query       The SQL for delete
     * @param queryFilter Query filter to prepared statement parameter binding.
     */
    public <T extends Object> int executeBatchDelete(String query, QueryFilter queryFilter)
            throws DataAccessException {

        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            doInternalBatchUpdate(queryFilter, preparedStatement);
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        } catch (SQLException e) {
            logDebug("Error in performing database delete: {} with parameters {}", query, queryFilter);
            throw new DataAccessException(JdbcConstants.ErrorCodes.ERROR_CODE_DATABASE_QUERY_PERFORMING_ERROR
                    .getErrorMessage() + query, e);
        }
        return 0;
    }

    /**
     * @return the driver name of the connection provided by the datasource.
     */
    public String getDriverName() throws DataAccessException {

        if (StringUtils.isNotBlank(driverName)) {
            return driverName;
        }

        try (Connection connection = dataSource.getConnection()) {
            driverName = connection.getMetaData().getDriverName();
            return driverName;
        } catch (SQLException e) {
            throw new DataAccessException(JdbcConstants.ErrorCodes.ERROR_CODE_GET_DB_TYPE.getErrorMessage(),
                    JdbcConstants.ErrorCodes.ERROR_CODE_GET_DB_TYPE.getErrorCode(), e);
        }
    }

    /**
     * Retrieve database product name from the data source.
     *
     * @return database product name from the data source.
     * @throws DataAccessException if an error occurs while retrieving metadata from data source.
     */
    public String getDatabaseProductName() throws DataAccessException {

        if (StringUtils.isNotBlank(productName)) {
            return productName;
        }

        try (Connection connection = dataSource.getConnection()) {
            productName = connection.getMetaData().getDatabaseProductName();
            return productName;
        } catch (SQLException e) {
            throw new DataAccessException(JdbcConstants.ErrorCodes.ERROR_CODE_GET_DB_TYPE.getErrorMessage(),
                    JdbcConstants.ErrorCodes.ERROR_CODE_GET_DB_TYPE.getErrorCode(), e);
        }
    }

    private int doInternalUpdate(QueryFilter queryFilter, PreparedStatement preparedStatement)
            throws SQLException, DataAccessException {

        if (queryFilter != null) {
            queryFilter.filter(preparedStatement);
        }
        return preparedStatement.executeUpdate();
    }

    private <T extends Object> void doInternalBatchUpdate(QueryFilter queryFilter, PreparedStatement
            preparedStatement) throws SQLException, DataAccessException {

        if (queryFilter != null) {
            queryFilter.filter(preparedStatement);
        }
        preparedStatement.executeBatch();
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
