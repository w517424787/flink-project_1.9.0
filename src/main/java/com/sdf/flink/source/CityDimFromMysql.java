package com.sdf.flink.source;

import com.sdf.flink.model.CityConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Modify by wwg 2020-06-05
 * 从mysql中读取维度数据
 */
public final class CityDimFromMysql extends RichSourceFunction<CityConfig> {
    private static Connection conn;
    private static PreparedStatement ps;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (conn == null) {
            conn = getConnection();
        }
        String sql = "SELECT cityId,cityDesc FROM dim_city;";
        ps = conn.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (conn != null) {
            conn.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    @Override
    public void run(SourceContext<CityConfig> ctx) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            CityConfig cityConfig = new CityConfig(resultSet.getInt("cityId"), resultSet.getString("cityDesc"));

            //获取数据
            ctx.collect(cityConfig);
        }
    }

    @Override
    public void cancel() {
        try {
            close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取连接串
     *
     * @return
     */
    private static Connection getConnection() {
        Connection connection = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://192.168.7.105:3306/flink_test?useUnicode=true&characterEncoding=UTF-8",
                    "root", "steve201718");
        } catch (Exception e) {
            System.out.println("mysql get connection has exception , msg = " + e.getMessage());
        }
        return connection;
    }
}
