package com.sdf.flink.table;

import com.sdf.flink.util.FilePathUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


public class FlinkSQLTopN {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final EnvironmentSettings evnSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        final StreamTableEnvironment tabEvn = StreamTableEnvironment.create(env, evnSettings);

        env.setParallelism(1);

        //测试数据
        final String filePath = FilePathUtil.getFilePath("topn.txt");
        //读取数据
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> dataStream =
                env.readTextFile(filePath).map(new MapFunction<String, Tuple4<String, String, String, Long>>() {
                    @Override
                    public Tuple4<String, String, String, Long> map(String value) throws Exception {
                        String[] arrays = value.split(",");
                        return new Tuple4<>(arrays[0], arrays[1],
                                arrays[2], Long.parseLong(arrays[3]));
                    }
                });

        //注册视图
        tabEvn.createTemporaryView("ShopSales", dataStream, "product_id, category, product_name, sales");
        // 选择每个分类中销量前3的产品
        String sqlQuery = "SELECT t.product_id,t.category,t.product_name,t.sales " +
                "FROM(" +
                " SELECT product_id,category,product_name,sales," +
                "        ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) AS rowNum " +
                " FROM ShopSales) AS t " +
                "WHERE t.rowNum <= 3";

        String sqlQuery2 = " SELECT product_id,category,product_name,sales" +
                " FROM ShopSales";
        Table result = tabEvn.sqlQuery(sqlQuery);
        tabEvn.toRetractStream(result, Row.class).print();


        env.execute("FlinkSQLTopN");
    }
}
