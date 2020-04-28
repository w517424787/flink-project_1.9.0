package com.sdf.flink.sink;

import com.sdf.flink.hbase.GetJSONDataIntoHBase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.util.ResourceBundle;


/**
 * Modify by wwg 2020-04-22
 * 将数据插入到HBase中
 */

public class SinkToHBase extends RichSinkFunction<String> {
    //这里必须用org.apache.hadoop.conf.Configuration
    private String flag;
    private static Admin admin;
    private static Connection conn;

    public SinkToHBase(String flag) {
        this.flag = flag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //初始化操作
        if (conn == null) {
            conn = getHBaseConnection();
        }
        if (admin == null && conn != null) {
            admin = conn.getAdmin();
        }
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        if (value != null && !"".equals(value)) {
            //解析json数据，插入到HBase表中
            GetJSONDataIntoHBase.getBuriedDataToHBase(value, admin, conn);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (conn != null) {
            admin.close();
            conn.close();
        }
    }

    /**
     * 获取HBase连接
     *
     * @return
     * @throws Exception
     */
    public Connection getHBaseConnection() throws Exception {
        //HBase Configuration
        org.apache.hadoop.conf.Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.rootdir", ResourceBundle.getBundle("config").getString(flag + "_hbase.rootdir"));
        hbaseConf.set("hbase.zookeeper.quorum", ResourceBundle.getBundle("config")
                .getString(flag + "_hbase.zookeeper.quorum"));
        hbaseConf.set("hbase.client.scanner.timeout.period",
                ResourceBundle.getBundle("config").getString(flag + "_hbase.client.scanner.timeout.period"));
        hbaseConf.set("hbase.rpc.timeout", ResourceBundle.getBundle("config")
                .getString(flag + "_hbase.rpc.timeout"));
        conn = ConnectionFactory.createConnection(hbaseConf);
        return conn;
    }
}
