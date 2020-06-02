package com.sdf.flink.streaming;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;


/**
 * Modify by wwg 2020-06-02
 * 通过读取mysql中的维表数据进行join
 * 通过市ID去转换市的名称
 * 如：cityID：5110，cityDesc：内江市
 */
public class FlinkStreamingDimJoin {

    /**
     * 根据输入的cityId获取对应的cityDesc
     * open方法先加载维度数据到内存中
     */
    private static final class DimFlatFunction extends RichFlatMapFunction<Integer, Tuple2<Integer, String>> {

        //cityId -> cityDesc
        static Map<Integer, String> dim = new HashMap<>();
        static Connection connection = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            try {
                if (connection == null) {
                    //获取连接
                    getConnection();

                    //读取数据
                    String sql = "SELECT cityId,cityDesc FROM dim_city;";
                    PreparedStatement statement = connection.prepareStatement(sql);
                    ResultSet result = statement.executeQuery();
                    while (result.next()) {
                        Integer cityId = result.getInt("cityId");
                        String cityDesc = result.getString("cityDesc");
                        //添加维度
                        dim.put(cityId, cityDesc);
                    }
                }
            } catch (Exception e) {
                System.out.println("read dim error!" + e.getMessage());
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (connection != null) {
                connection.close();
            }
        }

        /**
         * 创建mysql连接串
         */
        private static void getConnection() {
            try {
                Class.forName("com.mysql.jdbc.Driver");
                String url = "jdbc:mysql://192.168.7.105:3306/flink_test?useUnicode=true&characterEncoding=UTF-8";
                String user = "root";
                String pwd = "steve201718";
                connection = DriverManager.getConnection(url, user, pwd);
            } catch (Exception e) {
                System.out.println("mysql get connection has exception , msg = " + e.getMessage());
            }
        }

        @Override
        public void flatMap(Integer value, Collector<Tuple2<Integer, String>> out) throws Exception {
            //out.collect(new Tuple2<>(value, dim.get(value)));
            out.collect(Tuple2.of(value, dim.get(value)));
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取数据
        Integer[] cityIds = {5101, 5103, 5104, 5105, 5106, 5107, 5108};
        List<Integer> cityList = Arrays.asList(cityIds);
        DataStream<Integer> dataStream = env.fromCollection(cityList);
        SingleOutputStreamOperator<Tuple2<Integer, String>> outputStream = dataStream.flatMap(new DimFlatFunction());

        outputStream.print();
        env.execute("Flink Dim Join");
    }
}
