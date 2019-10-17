package com.sdf.flink.streaming;

import com.sdf.flink.util.GetKafkaProperties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class GetDataFromKafka {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //kafka属性
        Properties props = GetKafkaProperties.getKafkaProperties();

        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer011<>
                ("metric", new SimpleStringSchema(), props)).setParallelism(1);
        dataStreamSource.print();

        env.execute("Flink add data source");
    }
}
