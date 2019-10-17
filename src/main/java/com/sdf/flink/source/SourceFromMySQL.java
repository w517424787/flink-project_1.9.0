package com.sdf.flink.source;

import com.sdf.flink.model.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 从Mysql中读取数据
 */
public class SourceFromMySQL extends RichSourceFunction<Student> {
    private PreparedStatement ps;
    private Connection conn;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        //查询Mysql数据
        if (conn == null) {
            conn = getConnection();
        }
        String sql = "select * from student";
        ps = conn.prepareStatement(sql);
    }

    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        ResultSet result = ps.executeQuery();
        while (result.next()) {
            Student student = new Student(
                    result.getInt("id"),
                    result.getString("name").trim(),
                    result.getString("password").trim(),
                    result.getInt("age"));

            //获取数据
            ctx.collect(student);
        }
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
    public void cancel() {
        try {
            close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Connection getConnection() {
        Connection connection = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://192.168.200.129:3306/flink?useUnicode=true&characterEncoding=UTF-8",
                    "root", "123456");
        } catch (Exception e) {
            System.out.println("mysql get connection has exception , msg = " + e.getMessage());
        }
        return connection;
    }
}