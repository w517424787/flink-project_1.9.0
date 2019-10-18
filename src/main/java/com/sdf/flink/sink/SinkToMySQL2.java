package com.sdf.flink.sink;

import com.sdf.flink.model.Student;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * 将批量将数据插入到Mysql中去
 * 通过DBCP连接池连接数据库MySQL，将数据写入MySQL中
 */
public class SinkToMySQL2 extends RichSinkFunction<List<Student>> {
    private PreparedStatement ps;
    private Connection connection;
    private BasicDataSource basicDataSource;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        basicDataSource = new BasicDataSource();
        connection = getConnection(basicDataSource);
        String sql = "insert into student(id,name,password,age) values(?,?,?,?)";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void invoke(List<Student> value, Context context) throws Exception {
        for (Student student : value) {
            ps.setInt(1, student.getId());
            ps.setString(2, student.getName());
            ps.setString(3, student.getPassword());
            ps.setInt(4, student.getAge());
            ps.addBatch();
        }
        int[] count = ps.executeBatch();//批量后执行
        System.out.println("成功了插入了" + count.length + "行数据");
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
    private static Connection getConnection(BasicDataSource basicDataSource) {
        basicDataSource.setDriverClassName("com.mysql.jdbc.Driver");
        basicDataSource.setUrl("jdbc:mysql://192.168.200.129:3306/flink?useUnicode=true&characterEncoding=UTF-8");
        basicDataSource.setUsername("root");
        basicDataSource.setPassword("123456");

        //设置连接池的一些参数
        basicDataSource.setInitialSize(10);
        basicDataSource.setMaxTotal(1000);
        basicDataSource.setMinIdle(2);

        Connection connection = null;
        try {
            connection = basicDataSource.getConnection();
            System.out.println("创建连接池：" + connection);
        } catch (Exception e) {
            System.out.println("mysql get connection has exception , msg = " + e.getMessage());
        }
        return connection;
    }
}
