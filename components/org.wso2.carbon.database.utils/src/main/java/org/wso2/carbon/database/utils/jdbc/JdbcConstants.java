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

/**
 * Constants for the jdbcTemplate.
 */
public class JdbcConstants {

    public static final String PRODUCT_NAME_POSTGRESQL = "PostgreSQL";

    /**
     * Error codes for the exceptions in jdbcTemplate.
     */
    public enum ErrorCodes {
        ERROR_CODE_DATABASE_QUERY_PERFORMING_ERROR("10000", "Error in performing Database query: '%s'"),
        ERROR_CODE_GET_DB_TYPE("10001", "Error while getting the database connection metadata."),
        ERROR_CODE_AUTO_GENERATED_ID_FAILURE("10002", "Creating the record failed with Auto-Generated ID, no ID " +
                "obtained."),
        ERROR_CODE_DATABASE_QUERY_PERFORMING("10003", "Error in performing Database query: '%s.'"),
        ERROR_CODE_GETTING_CONNECTION_FAILURE("10004", "Could not get the connection from the datasource."),
        ERROR_CODE_ROLLBACK_TRANSACTION_FAILURE("10005", "Could not rollback the transaction."),
        ERROR_CODE_COMMIT_TRANSACTION_FAILURE("10006", "Could not commit the transaction."),
        ERROR_CODE_CLOSE_CONNECTION_FAILURE("10007", "Could not close the connection."),
        ERROR_CODE_MORE_RECORDS_IN_SINGLE_FETCH("10008", "There are more records than one found for query: ");

        final String errorCode;
        final String errorMessage;

        ErrorCodes(String errorCode, String errorMessage) {

            this.errorCode = errorCode;
            this.errorMessage = errorMessage;
        }

        public String getErrorCode() {

            return errorCode;
        }

        public String getErrorMessage() {

            return errorMessage;
        }
    }
}
