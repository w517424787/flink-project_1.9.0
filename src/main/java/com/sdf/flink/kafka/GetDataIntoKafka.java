package com.sdf.flink.kafka;

import com.sdf.flink.util.KafkaUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

public class GetDataIntoKafka {
    public static void main(String[] args) throws Exception {
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration conf = new Configuration();
        //端口：8081
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        final String brokerList = "192.168.7.113:9092,192.168.7.114:9092,192.168.7.115:9092";
        final String topic = "flink-test";

        //读取Kafka属性
        Properties properties = KafkaUtils.getKafkaProperties();

        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer011<>("usernetworkdelay",
                new SimpleStringSchema(), properties)).setParallelism(1);

        dataStreamSource.addSink(new FlinkKafkaProducer011<>(brokerList, topic, new SimpleStringSchema()))
                .name("flink-demo").setParallelism(1);

        env.execute("flink demo");
    }
}
