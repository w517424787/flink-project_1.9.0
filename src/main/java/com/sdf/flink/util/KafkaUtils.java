package com.sdf.flink.util;

import java.util.Properties;

public class KafkaUtils {

    /**
     * 读取kafka属性
     * @return
     */
    public static Properties getKafkaProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.7.113:9092,192.168.7.114:9092,192.168.7.115:9092");
        //动态感知kafka主题分区的增加  单位毫秒
        //properties.setProperty("flink.partition-discovery.interval-millis", "5000");
        //kafka 0.8才需要
        //properties.put("zookeeper.connect", "192.168.7.110:2181,192.168.7.111:2181,192.168.7.112:2181");
        properties.put("group.id", "flink-group");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "latest");
        return properties;
    }
}
