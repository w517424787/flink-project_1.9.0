package com.sdf.flink.sql;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sdf.flink.util.KafkaConfigUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * Modify by wwg 2020-04-30
 * 通过Flink sql对读取的Kafka数据进行count
 * 通过2min的滚动窗口进行统计
 * kafka数据格式：
 * {"appId":9999,"id":17012285,"playerId":1000001,"collectTime":"2019-12-10 14:28:08",
 * "tableName":"dwm.m_f_game_buried_point_stat","clickType":"20001.3.1.1.7",
 * "devicecode":"DESKTOP-OIH0F6Nxaasa","serverVersion":"TYF","os":4,"param":"2002.1"}
 */

public class FlinkSqlWindowCount {
    public static void main(String[] args) throws Exception {
        final String broker_list = "192.168.7.113:9092,192.168.7.114:9092,192.168.7.115:9092";
        final String group_id = "group_usernetworkdelay_sql";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(3600000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        //流表
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //kafka属性
        Properties properties = KafkaConfigUtil.buildConsumerProps(broker_list, group_id);
        @SuppressWarnings("unchecked")
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer("usernetworkdelay",
                new SimpleStringSchema(), properties);
        //设置offset读取位置
        consumer.setStartFromGroupOffsets(); //默认从group.id中读取

        //读取数据
        DataStreamSource<String> dataStreamSource = env.addSource(consumer);

        //将json格式转换成Tuple格式
        DataStream<Tuple6<String, String, String, String, String, String>> ds = dataStreamSource.map(new JSONToTupleMap())
                .setParallelism(1);

        //ds.print();
        //注册表
        tableEnv.createTemporaryView("buried_table", ds,
                "tablename,playerid,clicktype,serverversion,param,collecttime,proctime.proctime");
        //分组查询
        //Table result = tableEnv.sqlQuery("select playerid,clicktype,count(1) as clickcount " +
        //"from buried_table where tablename = 'dwm.m_f_game_buried_point_stat' group by playerid,clicktype");
        //Table 转化为 DataStream
        //DataStream<Tuple3<String, String, Long>> appendStream = tableEnv.toAppendStream(result,
        //Types.TUPLE(Types.STRING, Types.STRING, Types.LONG));

        //tableEnv.toRetractStream(result,Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)).print();

        //appendStream.print();

        //每2min统计一下
        Table result = tableEnv.sqlQuery("select playerid,TUMBLE_END(proctime, INTERVAL '2' MINUTE) as proctime,count(1) as clickcount " +
                "from buried_table " +
                "where tablename = 'dwm.m_f_game_buried_point_stat' AND collecttime >= '2020-04-30'" +
                "group by TUMBLE(proctime, INTERVAL '2' MINUTE),playerid");

        tableEnv.toRetractStream(result, Types.TUPLE(Types.STRING, Types.SQL_TIMESTAMP, Types.LONG)).print();

        env.execute("Flink_SQL");

    }

    public static class JSONToTupleMap implements MapFunction<String, Tuple6<String, String, String, String, String, String>> {

        @Override
        public Tuple6<String, String, String, String, String, String> map(String value) throws Exception {
            Tuple6<String, String, String, String, String, String> tuple6 = null;
            if (value != null && !value.isEmpty()) {
                JSONObject jsonObject = JSON.parseObject(value.toLowerCase());
                String tableName = jsonObject.getString("tablename");
                String playerId = jsonObject.getString("playerid");
                String clickType = jsonObject.getString("clicktype");
                String serverVersion = jsonObject.getString("serverversion");
                String param = jsonObject.getString("param");
                String collectTime = jsonObject.getString("collecttime");

                tuple6 = new Tuple6<>(tableName, playerId, clickType, serverVersion, param, collectTime);
            }
            return tuple6;
        }
    }
}
