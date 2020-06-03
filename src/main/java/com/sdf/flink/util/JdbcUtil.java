package com.sdf.flink.util;

import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 获取mysql连接串
 */
public class JdbcUtil {
    private static BasicDataSource dataSource = new BasicDataSource();

    static {
        initSourceConfig();
    }

    private static void initSourceConfig() {
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUsername("root");
        dataSource.setPassword("steve201718");
        dataSource.setUrl("jdbc:mysql://192.168.7.105:3306/flink_test?useUnicode=true&characterEncoding=UTF-8");
        dataSource.setInitialSize(20);
        dataSource.setMaxTotal(80);
        dataSource.setMaxIdle(40);
        dataSource.setMinIdle(20);
        dataSource.setMaxWaitMillis(6000);
    }

    public static BasicDataSource getDataSource() {
        return dataSource;
    }

    /**
     * 获取连接
     *
     * @return
     */
    public static Connection getConnection() {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 关闭连接串
     *
     * @param conn
     * @param st
     * @param rs
     */
    public static void release(Connection conn, Statement st, ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (st != null) {
            try {
                st.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (conn != null) {
            try {
                //将Connection连接对象还给数据库连接池
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
