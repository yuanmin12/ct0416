package com.atguigu.Utils;

import java.sql.*;

public class JDBCUtil {

    private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private static final String MYSQL_URL = "jdbc:mysql://hadoop102:3306/ct?useUnicode=true&characterEncoding=UTF-8";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "000000";

    private static Connection connection = null;

    //获取JDBC连接
    private static Connection getConnection() {

        Connection connection = null;
        try {
            Class.forName(MYSQL_DRIVER);
            connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    //关闭JDBC相关资源
    public static void close(Connection connection, PreparedStatement preparedStatement, ResultSet resultSet) {

        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    //获取连接的单例
    public static Connection getInstance() {
        if (connection == null) {
            synchronized (JDBCUtil.class) {
                if (connection == null) {
                    connection = getConnection();
                }
            }
        }
        return connection;
    }
}
