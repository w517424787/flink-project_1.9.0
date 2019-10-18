package com.sdf.flink.sink;

import com.sdf.flink.model.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 将数据插入到Mysql中去
 */
public class SinkToMySQL extends RichSinkFunction<Student> {
    private PreparedStatement ps;
    private Connection connection;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "insert into student(id,name,password,age) values(?,?,?,?)";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void invoke(Student value, Context context) throws Exception {
        ps.setInt(1, value.getId());
        ps.setString(2, value.getName());
        ps.setString(3, value.getPassword());
        ps.setInt(4, value.getAge());

        //插入数据
        ps.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (ps != null) {
            ps.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * 打开Mysql连接串
     *
     * @return
     */
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
