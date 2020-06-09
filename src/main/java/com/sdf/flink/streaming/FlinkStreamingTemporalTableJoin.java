package com.sdf.flink.streaming;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class FlinkStreamingTemporalTableJoin {

    //水印
    final static class OrderTimestampExtractor<T1, T2> extends BoundedOutOfOrdernessTimestampExtractor<Tuple3<T1, T2, Timestamp>> {

        public OrderTimestampExtractor(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple3<T1, T2, Timestamp> element) {
            return element.f2.getTime();
        }
    }

    public static void main(String[] args) throws Exception {
        final Logger LOG = LoggerFactory.getLogger(FlinkStreamingTemporalTableJoin.class);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //构建订单数据
        List<Tuple3<Long, String, Timestamp>> ordersData = new ArrayList<>();
        ordersData.add(Tuple3.of(2L, "Euro", new Timestamp(2L)));
        ordersData.add(Tuple3.of(1L, "US Dollar", new Timestamp(3L)));
        ordersData.add(Tuple3.of(50L, "Yen", new Timestamp(4L)));
        ordersData.add(Tuple3.of(3L, "Euro", new Timestamp(5L)));

        //订单流
        SingleOutputStreamOperator<Tuple3<Long, String, Timestamp>> orderStream = env.fromCollection(ordersData)
                .assignTimestampsAndWatermarks(new OrderTimestampExtractor(Time.seconds(10)));
        //转行成Table
        Table orderTable = tableEnv.fromDataStream(orderStream, "amount,currency,rowtime.rowtime");
        //注册视图
        tableEnv.createTemporaryView("Orders", orderTable);

        //构建汇率数据
        List<Tuple3<String, Long, Timestamp>> ratesData = new ArrayList<>();
        ratesData.add(Tuple3.of("US Dollar", 102L, new Timestamp(1L)));
        ratesData.add(Tuple3.of("Euro", 114L, new Timestamp(1L)));
        ratesData.add(Tuple3.of("Yen", 1L, new Timestamp(1L)));
        ratesData.add(Tuple3.of("Euro", 116L, new Timestamp(5L)));
        ratesData.add(Tuple3.of("Euro", 119L, new Timestamp(7L)));

        //汇率流
        SingleOutputStreamOperator<Tuple3<String, Long, Timestamp>> rateStream = env.fromCollection(ratesData)
                .assignTimestampsAndWatermarks(new OrderTimestampExtractor(Time.seconds(10)));
        //转行成Table
        Table rateTable = tableEnv.fromDataStream(rateStream, "currency,rate,rowtime.rowtime");
        //注册视图
        tableEnv.createTemporaryView("RatesHistory", rateTable);
        //创建TemporalTableFunction
        //tableEnv.scan("RatesHistory");
        TemporalTableFunction temporalTableFunction = rateTable.createTemporalTableFunction("rowtime", "currency");
        //注册TemporalTableFunction
        tableEnv.registerFunction("Rates", temporalTableFunction);
        //查询sql
        String query = "SELECT o.currency,o.amount,r.rate,o.amount * r.rate AS yen_amount" +
                " FROM Orders As o,LATERAL TABLE (Rates(o.rowtime)) AS r " +
                " WHERE r.currency = o.currency";
        //tableEnv.createTemporaryView("TemporalJoinResult", tableEnv.sqlQuery(query));
        //输出数据
        Table result = tableEnv.sqlQuery(query);
        tableEnv.toAppendStream(result, Row.class).print();

        env.execute("Temporal Table Join");
    }
}
