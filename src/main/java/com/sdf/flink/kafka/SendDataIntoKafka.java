package com.sdf.flink.kafka;

import com.sdf.flink.source.MyKafkaSource;
import com.sdf.flink.util.KafkaConfigUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class SendDataIntoKafka {
    public static void main(String[] args) throws Exception {
        final String broker_list = "192.168.7.113:9092,192.168.7.114:9092,192.168.7.115:9092";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.addSource(new MyKafkaSource()).setParallelism(1);

        //读取Kafka属性
        Properties properties = KafkaConfigUtil.buildProducerProps(broker_list);
        @SuppressWarnings("unchecked")
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer("topn", new SimpleStringSchema(), properties);

        //保存数据
        text.addSink(producer).setParallelism(1);
        env.execute("kafka data");
    }
}
