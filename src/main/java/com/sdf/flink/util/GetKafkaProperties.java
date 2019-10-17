package com.sdf.flink.util;

import java.util.Properties;

public class GetKafkaProperties {

    /**
     * 获取kafka属性
     * @return
     */
    public static Properties getKafkaProperties() {
        //kafka属性
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.200.129:9092");
        props.put("zookeeper.connect", "192.168.200.129:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put("auto.offset.reset", "latest");
        props.put("auto.offset.reset", "earliest");
        return props;
    }
}
