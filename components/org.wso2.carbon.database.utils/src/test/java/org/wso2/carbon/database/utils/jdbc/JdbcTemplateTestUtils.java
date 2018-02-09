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
import org.apache.commons.lang3.StringUtils;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Provides the utilities to the test methods of this component.
 */
public class JdbcTemplateTestUtils {

    BasicDataSource dataSource;

    /**
     * Returns the path of the provided fileName for the method.
     * @param fileName required name of the file.
     * @return file path string of the provided file name.
     */
    public static String getFilePath(String fileName) {

        if (StringUtils.isNotBlank(fileName)) {
            return Paths.get(System.getProperty("user.dir"), "src", "test", "resources", "jdbcTemplateScript", fileName)
                    .toString();
        }
        return null;
    }

    /**
     * Initialize the H@ database for the test methods.
     * @param databaseName name of the required database.
     * @param scriptPath path of the script that contains the SQL queries of the tables to be executed.
     */
    public void initH2Database(String databaseName, String scriptPath) throws SQLException {

        dataSource = new BasicDataSource();
        dataSource.setDriverClassName("org.h2.Driver");
        dataSource.setUsername("userName");
        dataSource.setPassword("password");
        dataSource.setUrl("jdbc:h2:mem:test" + databaseName);
        try (Connection connection = dataSource.getConnection()) {
            connection.createStatement().executeUpdate("RUNSCRIPT FROM '" + scriptPath + "'");
        }
    }

    public BasicDataSource getDataSource() throws SQLException {

        return dataSource;
    }
}
