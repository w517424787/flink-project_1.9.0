package com.sdf.flink.kafka;

import com.sdf.flink.util.KafkaUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import javax.annotation.Nullable;
import java.util.Properties;

public class GetDataIntoKafka {
    public static void main(String[] args) throws Exception {
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration conf = new Configuration();
        //端口：8081
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        final String brokerList = "192.168.7.113:9092,192.168.7.114:9092,192.168.7.115:9092";
        final String topic = "flink-TmpDirTest";

        //读取Kafka属性
        Properties properties = KafkaUtils.getKafkaProperties();

        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<>("usernetworkdelay",
                new SimpleStringSchema(), properties)).setParallelism(1);

//        dataStreamSource.addSink(new FlinkKafkaProducer<>(brokerList, topic, new SimpleStringSchema()))
//                .name("flink-demo").setParallelism(1);

        //输出到kafka中，EXACTLY_ONCE模式
        dataStreamSource.addSink(new FlinkKafkaProducer<>(topic,
                new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).setParallelism(1);

//        dataStreamSource.addSink(new FlinkKafkaProducer<>(topic,
//                new KafkaSerializationSchema<String>() {
//                    @Override
//                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
//                        return new ProducerRecord<>(topic, element.getBytes());
//                    }
//                }, properties,
//                FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).setParallelism(1);

        env.execute("flink demo");
    }
}
