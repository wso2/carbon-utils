/*
 *  Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.wso2.carbon.database.utils.jdbc;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * Prepared statement with named indexes.
 */
@SuppressFBWarnings(value = "SQL_INJECTION_JDBC")
public class NamedPreparedStatement implements PreparedStatement {

    private final PreparedStatement preparedStatement;
    private final List<String> fields = new ArrayList<>();

    /**
     * Create a named prepared statement with repeated indexes.
     *
     * @param connection Database connection to be used.
     * @param sqlQuery   Underlying SQL query.
     * @param repetition Repetition of given index.
     * @throws SQLException SQL Exception.
     */
    public NamedPreparedStatement(Connection connection, String sqlQuery, Map<String, Integer> repetition)
            throws SQLException {

        preparedStatement = constructPreparedStatement(connection, sqlQuery, repetition, null);
    }

    /**
     * Create a named prepared statement.
     *
     * @param connection    Database connection to be used.
     * @param sqlQuery      Underlying SQL query.
     * @throws SQLException SQL Exception.
     */
    public NamedPreparedStatement(Connection connection, String sqlQuery) throws SQLException {

        this(connection, sqlQuery, new HashMap<>());
    }

    /**
     * Create a named prepared statement with a specified column value to be returned.
     *
     * @param connection            Database connection to be used.
     * @param sqlQuery              Underlying SQL query.
     * @param returningColumnName   Name of the unique column to be returned upon query success.
     * @throws SQLException         SQL Exception.
     */
    public NamedPreparedStatement(Connection connection, String sqlQuery, String returningColumnName)
            throws SQLException {

        this(connection, sqlQuery, new HashMap<>(), returningColumnName);
    }

    /**
     * Create a named prepared statement with repeated indexes and a specified column value to be returned.
     *
     * @param connection            Database connection to be used.
     * @param sqlQuery              Underlying SQL query.
     * @param repetition            Repetition of given index.
     * @param returningColumnName   Name of the column to be returned upon query success.
     * @throws SQLException         SQL Exception.
     */
    public NamedPreparedStatement(Connection connection, String sqlQuery, Map<String, Integer> repetition,
                                  String returningColumnName) throws SQLException {

        preparedStatement = constructPreparedStatement(connection, sqlQuery, repetition, returningColumnName);
    }

    /**
     * Set <code>long</code> value for the named index.
     *
     * @param name  Name of the index.
     * @param value Value to be replaced.
     * @throws SQLException SQL Exception.
     */
    public void setLong(String name, long value) throws SQLException {

        for (int index : getIndexList(name)) {
            preparedStatement.setLong(index, value);
        }
    }

    /**
     * Set <code>int</code> value for the named index.
     *
     * @param name  Name of the index.
     * @param value Value to be replaced.
     * @throws SQLException SQL Exception.
     */
    public void setInt(String name, int value) throws SQLException {

        for (int index : getIndexList(name)) {
            preparedStatement.setInt(index, value);
        }
    }

    /**
     * Set <code>String</code> value for the named index.
     *
     * @param name  Name of the index.
     * @param value Value to be replaced.
     * @throws SQLException SQL Exception
     */
    public void setString(String name, String value) throws SQLException {

        for (int index : getIndexList(name)) {
            preparedStatement.setString(index, value);
        }
    }

    /**
     * Replace repeated indexes with the list of values.
     *
     * @param name   Name of the index.
     * @param values Values to be replaced.
     * @throws SQLException SQL Exception.
     */
    public void setString(String name, List<String> values) throws SQLException {

        int indexInc = 0;
        for (String value : values) {
            preparedStatement.setString(getIndex(name) + indexInc, value);
            indexInc++;
        }
    }

    public void setObject(String name, Object value) throws SQLException {

        for (int index : getIndexList(name)) {
            preparedStatement.setObject(index, value);
        }
    }

    /**
     * Set <code>TimeStamp</code> value for the named index.
     *
     * @param name      Name of the index.
     * @param timestamp value to be replaced.
     * @param calendar  value to be replaced.
     * @throws SQLException SQL Exception.
     */
    public void setTimeStamp(String name, Timestamp timestamp, Calendar calendar) throws SQLException {

        for (int index : getIndexList(name)) {
            preparedStatement.setTimestamp(index, timestamp, calendar);
        }
    }

    /**
     * Set <code>boolean</code> value for the named index.
     *
     * @param name  Name of the index.
     * @param value Value to be replaced.
     * @throws SQLException SQL Exception.
     */
    public void setBoolean(String name, boolean value) throws SQLException {

        for (int index : getIndexList(name)) {
            preparedStatement.setBoolean(index, value);
        }
    }

    private int getIndex(String name) {

        return fields.indexOf(name) + 1;
    }

    private List<Integer> getIndexList(String name) {

        List<Integer> indexList = new ArrayList<>();
        for (int index = 0; index < fields.size(); index++) {
            if (fields.get(index).equals(name)) {
                indexList.add(index + 1);
            }
        }
        return indexList;
    }

    private PreparedStatement constructPreparedStatement(Connection connection, String sqlQuery,
                                                         Map<String, Integer> repetition, String returningColumnName)
            throws SQLException {

        int pos;
        while ((pos = sqlQuery.indexOf(":")) != -1) {

            int end = sqlQuery.substring(pos).indexOf(";");
            if (end == -1) {
                throw new SQLException("Cannot find the end of the placeholder.");
            } else {
                end += pos;
            }

            fields.add(sqlQuery.substring(pos + 1, end));
            StringBuilder builder = new StringBuilder("?");

            if (repetition.get(sqlQuery.substring(pos + 1, end)) != null) {
                for (int i = 0; i < repetition.get(sqlQuery.substring(pos + 1, end)) - 1; i++) {
                    builder.append(", ?");
                }
            }

            sqlQuery = String.format("%s %s %s", sqlQuery.substring(0, pos), builder,
                    sqlQuery.substring(end + 1));
        }

        PreparedStatement prepStatement;
        if (returningColumnName == null) {
            prepStatement = connection.prepareStatement(sqlQuery);
        } else {
            prepStatement = connection.prepareStatement(sqlQuery, new String[]
                    {JdbcUtils.getConvertedAutoGeneratedColumnName(connection.getMetaData()
                            .getDatabaseProductName(), returningColumnName)});
        }
        return prepStatement;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {

        return preparedStatement.executeQuery();
    }

    @Override
    public int executeUpdate() throws SQLException {

        return preparedStatement.executeUpdate();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {

        preparedStatement.setNull(parameterIndex, sqlType);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean value) throws SQLException {

        preparedStatement.setBoolean(parameterIndex, value);
    }

    @Override
    public void setByte(int parameterIndex, byte value) throws SQLException {

        preparedStatement.setByte(parameterIndex, value);
    }

    @Override
    public void setShort(int parameterIndex, short value) throws SQLException {

        preparedStatement.setShort(parameterIndex, value);
    }

    @Override
    public void setInt(int parameterIndex, int value) throws SQLException {

        preparedStatement.setInt(parameterIndex, value);
    }

    @Override
    public void setLong(int parameterIndex, long value) throws SQLException {

        preparedStatement.setLong(parameterIndex, value);
    }

    @Override
    public void setFloat(int parameterIndex, float value) throws SQLException {

        preparedStatement.setFloat(parameterIndex, value);
    }

    @Override
    public void setDouble(int parameterIndex, double value) throws SQLException {

        preparedStatement.setDouble(parameterIndex, value);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal bigDecimal) throws SQLException {

        preparedStatement.setBigDecimal(parameterIndex, bigDecimal);
    }

    @Override
    public void setString(int parameterIndex, String string) throws SQLException {

        preparedStatement.setString(parameterIndex, string);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] bytes) throws SQLException {

        preparedStatement.setBytes(parameterIndex, bytes);
    }

    @Override
    public void setDate(int parameterIndex, Date date) throws SQLException {

        preparedStatement.setDate(parameterIndex, date);
    }

    @Override
    public void setTime(int parameterIndex, Time time) throws SQLException {

        preparedStatement.setTime(parameterIndex, time);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp timestamp) throws SQLException {

        preparedStatement.setTimestamp(parameterIndex, timestamp);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream inputStream, int length) throws SQLException {

        preparedStatement.setAsciiStream(parameterIndex, inputStream, length);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream inputStream, int length) throws SQLException {

        preparedStatement.setUnicodeStream(parameterIndex, inputStream, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream inputStream, int length) throws SQLException {

        preparedStatement.setBinaryStream(parameterIndex, inputStream, length);
    }

    @Override
    public void clearParameters() throws SQLException {

        preparedStatement.clearParameters();
    }

    @Override
    public void setObject(int parameterIndex, Object object, int targetSqlType) throws SQLException {

        preparedStatement.setObject(parameterIndex, object, targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object object) throws SQLException {

        preparedStatement.setObject(parameterIndex, object);
    }

    @Override
    public boolean execute() throws SQLException {

        return preparedStatement.execute();
    }

    @Override
    public void addBatch() throws SQLException {

        preparedStatement.addBatch();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {

        preparedStatement.setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setRef(int parameterIndex, Ref ref) throws SQLException {

        preparedStatement.setRef(parameterIndex, ref);
    }

    @Override
    public void setBlob(int parameterIndex, Blob blob) throws SQLException {

        preparedStatement.setBlob(parameterIndex, blob);
    }

    @Override
    public void setClob(int parameterIndex, Clob clob) throws SQLException {

        preparedStatement.setClob(parameterIndex, clob);
    }

    @Override
    public void setArray(int parameterIndex, Array array) throws SQLException {

        preparedStatement.setArray(parameterIndex, array);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {

        return preparedStatement.getMetaData();
    }

    @Override
    public void setDate(int parameterIndex, Date date, Calendar cal) throws SQLException {

        preparedStatement.setDate(parameterIndex, date, cal);
    }

    @Override
    public void setTime(int parameterIndex, Time time, Calendar cal) throws SQLException {

        preparedStatement.setTime(parameterIndex, time, cal);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp timestamp, Calendar cal) throws SQLException {

        preparedStatement.setTimestamp(parameterIndex, timestamp, cal);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {

        preparedStatement.setNull(parameterIndex, sqlType, typeName);
    }

    @Override
    public void setURL(int parameterIndex, URL url) throws SQLException {

        preparedStatement.setURL(parameterIndex, url);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {

        return preparedStatement.getParameterMetaData();
    }

    @Override
    public void setRowId(int parameterIndex, RowId rowId) throws SQLException {

        preparedStatement.setRowId(parameterIndex, rowId);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {

        preparedStatement.setNString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {

        preparedStatement.setNCharacterStream(parameterIndex, value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {

        preparedStatement.setNClob(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {

        preparedStatement.setClob(parameterIndex, reader, length);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {

        preparedStatement.setBlob(parameterIndex, inputStream, length);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {

        preparedStatement.setNClob(parameterIndex, reader, length);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {

        preparedStatement.setSQLXML(parameterIndex, xmlObject);
    }

    @Override
    public void setObject(int parameterIndex, Object object, int targetSqlType, int scaleOrLength) throws SQLException {

        preparedStatement.setObject(parameterIndex, object, targetSqlType, scaleOrLength);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream inputStream, long length) throws SQLException {

        preparedStatement.setAsciiStream(parameterIndex, inputStream, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream inputStream, long length) throws SQLException {

        preparedStatement.setBinaryStream(parameterIndex, inputStream, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {

        preparedStatement.setCharacterStream(parameterIndex, reader, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream inputStream) throws SQLException {

        preparedStatement.setAsciiStream(parameterIndex, inputStream);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream inputStream) throws SQLException {

        preparedStatement.setBinaryStream(parameterIndex, inputStream);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {

        preparedStatement.setCharacterStream(parameterIndex, reader);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {

        preparedStatement.setNCharacterStream(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {

        preparedStatement.setClob(parameterIndex, reader);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {

        preparedStatement.setBlob(parameterIndex, inputStream);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {

        preparedStatement.setNClob(parameterIndex, reader);
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {

        return preparedStatement.executeQuery(sql);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {

        return preparedStatement.executeUpdate(sql);
    }

    @Override
    public void close() throws SQLException {

        preparedStatement.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {

        return preparedStatement.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {

        preparedStatement.setMaxFieldSize(max);
    }

    @Override
    public int getMaxRows() throws SQLException {

        return preparedStatement.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {

        preparedStatement.setMaxFieldSize(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

        preparedStatement.setEscapeProcessing(enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {

        return preparedStatement.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {

        preparedStatement.setQueryTimeout(seconds);
    }

    @Override
    public void cancel() throws SQLException {

        preparedStatement.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {

        return preparedStatement.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {

        preparedStatement.clearWarnings();
    }

    @Override
    public void setCursorName(String name) throws SQLException {

        preparedStatement.setCursorName(name);
    }

    @Override
    public boolean execute(String sql) throws SQLException {

        return preparedStatement.execute(sql);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {

        return preparedStatement.getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {

        return preparedStatement.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {

        return preparedStatement.getMoreResults();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

        preparedStatement.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {

        return preparedStatement.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

        preparedStatement.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {

        return preparedStatement.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {

        return preparedStatement.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {

        return preparedStatement.getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {

        preparedStatement.addBatch(sql);
    }

    @Override
    public void clearBatch() throws SQLException {

        preparedStatement.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {

        return preparedStatement.executeBatch();
    }

    @Override
    public Connection getConnection() throws SQLException {

        return preparedStatement.getConnection();
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {

        return preparedStatement.getMoreResults();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {

        return preparedStatement.getGeneratedKeys();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {

        return preparedStatement.executeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {

        return preparedStatement.executeUpdate(sql, columnIndexes);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {

        return preparedStatement.executeUpdate(sql, columnNames);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {

        return preparedStatement.execute(sql, autoGeneratedKeys);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {

        return preparedStatement.execute(sql, columnIndexes);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {

        return preparedStatement.execute(sql, columnNames);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {

        return preparedStatement.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {

        return preparedStatement.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {

        preparedStatement.setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {

        return preparedStatement.isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {

        preparedStatement.closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {

        return preparedStatement.isCloseOnCompletion();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {

        return preparedStatement.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {

        return preparedStatement.isWrapperFor(iface);
    }
}
