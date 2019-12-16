package com.sdf.flink.kafka;

import com.sdf.flink.util.KafkaUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * 简单读取kafka中某个topic的数据
 */
public class GetDataFromKafka {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        Properties properties = KafkaUtils.getKafkaProperties();
//
//        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer011<>("usernetworkdelay",
//                new SimpleStringSchema(), properties)).setParallelism(1);
//        dataStreamSource.print();

        env.execute("Flink Demo");
    }
}
