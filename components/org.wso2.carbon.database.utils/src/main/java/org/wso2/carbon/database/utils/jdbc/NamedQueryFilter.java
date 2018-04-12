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

import java.sql.SQLException;

/**
 * SQL variable binding.
 * e.g.
 * <code>
 * "FROM IDP WHERE NAME=?";
 * this.jdbcNamedTemplate.fetchSingleRecord(GET_ALL_IDP_SQL, (resultSet, rowNumber) -> {
 * ...
 * return ..;
 * },(namedPreparedStatement) -> {
 * namedPreparedStatement.setString("name", var1);
 * });
 * </code>
 */
@FunctionalInterface
public interface NamedQueryFilter {

    /**
     * Performs the variable binding.
     *
     * @param namedPreparedStatement
     * @throws SQLException
     */
    void filter(NamedPreparedStatement namedPreparedStatement) throws SQLException;
}
