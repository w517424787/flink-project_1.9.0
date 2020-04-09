package com.sdf.flink.util;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * kafka相关属性
 */
public class KafkaConfigUtil {
    private static final int RETRIES_CONFIG = 3;
    //public static String defaultBroker_list = "192.168.7.113:9092,192.168.7.114:9092,192.168.7.115:9092";

    /**
     * 消费数据
     * @param broker_list
     * @param groupId
     * @return
     */
    public static Properties buildConsumerProps(String broker_list, String groupId) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker_list);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "5000");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "20000");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS,
                Long.toString(TimeUnit.MINUTES.toMillis(1)));

        return properties;
    }


    /**
     * 生产数据
     * @param broker_list
     * @return
     */
    public static Properties buildProducerProps(String broker_list){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker_list);
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(RETRIES_CONFIG));
        properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "300000");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "3");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");

        return properties;
    }
}
