package com.sdf.flink.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * Modify by wwg 2020-04-27
 * HBase相关数据操作
 */

public class HBaseClient {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseClient.class);

    /**
     * 创建HBase表
     *
     * @param admin          admin
     * @param tableName      表名
     * @param columnFamilies 列簇
     */
    public static void createTable(Admin admin, String tableName, String... columnFamilies) throws IOException {
        TableName hbaseTable = TableName.valueOf(tableName);
        //判断表是否存在
        if (admin.tableExists(hbaseTable)) {
            LOG.info("Table Exists!");
        } else {
            HTableDescriptor tableDescriptor = new HTableDescriptor(hbaseTable);
            for (String columnFamily : columnFamilies) {
                tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
            }
            admin.createTable(tableDescriptor);
            LOG.info("Create table success!");
        }
    }

    /**
     * 插入数据到HBase表中
     *
     * @param admin     admin
     * @param conn      conn
     * @param tableName 表名
     * @param rowKey    主键
     * @param dataMap   (cf,((column,value),(column,value),(column,value)))，可能一次会插入多个列簇数据
     */
    public static void addData(Admin admin, Connection conn, String tableName, String rowKey,
                               Map<String, Map<String, String>> dataMap) throws IOException {
        //先判断HBase表是否存在
        TableName hbaseTable = TableName.valueOf(tableName);
        if (!admin.tableExists(hbaseTable)) {
            //默认定义一个列簇：info
            createTable(admin, tableName, "info");
        }
        Table table = conn.getTable(hbaseTable);
        Put put = new Put(rowKey.getBytes());
        // ("cf",(("name","tom"),("age",25)))
        for (String cf : dataMap.keySet()) {
            //每个columnFamily下的column和value，每次可以插入多个column和value
            Map<String, String> columnMap = dataMap.get(cf);
            for (String column : columnMap.keySet()) {
                put.addColumn(cf.getBytes(), column.getBytes(), columnMap.get(column).getBytes());
            }
        }
        table.put(put);
    }

    /**
     * 查询HBase数据
     *
     * @param conn
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param column
     */
    public static String getData(Connection conn, String tableName,
                                 String rowKey, String familyName, String column) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        byte[] row = rowKey.getBytes();
        Get get = new Get(row);
        Result result = table.get(get);
        byte[] resultValue = result.getValue(familyName.getBytes(), column.getBytes());
        return resultValue == null ? null : new String(resultValue);
    }

    /**
     * 取出表中所有的key
     *
     * @param conn
     * @param tableName
     * @return
     */
    public static List<String> getAllKey(Connection conn, String tableName) throws IOException {
        List<String> keys = new ArrayList<>();
        Scan scan = new Scan();
        Table table = conn.getTable(TableName.valueOf(tableName));
        ResultScanner scanner = table.getScanner(scan);
        for (Result r : scanner) {
            keys.add(new String(r.getRow()));
        }
        return keys;
    }
}
