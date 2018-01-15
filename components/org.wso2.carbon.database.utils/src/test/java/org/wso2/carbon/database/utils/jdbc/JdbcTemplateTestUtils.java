package org.wso2.carbon.database.utils.jdbc;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang3.StringUtils;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;

public class JdbcTemplateTestUtils {
    BasicDataSource dataSource;

    public static String getFilePath(String fileName) {
        if (StringUtils.isNotBlank(fileName)) {
            return Paths.get(System.getProperty("user.dir"), "src", "test", "resources", "jdbcTemplateScript", fileName)
                    .toString();
        }
        return null;
    }

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
