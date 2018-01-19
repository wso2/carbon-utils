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

import java.sql.Connection;

/**
 * Class that keeps the states of a transaction.
 */
public class TransactionEntry {

    private Connection connection;
    private int transactionDepth = 0;
    private boolean autoCommitState;

    public Connection getConnection() {

        return connection;
    }

    public void setConnection(Connection connection) {

        this.connection = connection;
    }

    public int getTransactionDepth() {

        return transactionDepth;
    }

    /**
     * Increment the transaction depth by one to store the level of a transaction.
     */
    public void incrementTransactionDepth() {

        transactionDepth++;
    }

    /**
     * Decrement the transaction depth by one to notice the remaining levels of a transaction.
     */
    public void decrementTransactionDepth() {

        transactionDepth--;
    }

    public boolean getAutoCommitState() {

        return autoCommitState;
    }

    public void setAutoCommitState(boolean autoCommitState) {

        this.autoCommitState = autoCommitState;
    }
}
