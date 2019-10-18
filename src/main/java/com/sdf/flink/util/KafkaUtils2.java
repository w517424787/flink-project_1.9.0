package com.sdf.flink.util;

import com.alibaba.fastjson.JSON;
import com.sdf.flink.model.Student;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 模拟student数据写入到kafka topic中
 */
public class KafkaUtils2 {
    private static final String kafkaBrokerList = "192.168.200.129:9092";
    private static final String kafkaTopic = "student";

    /**
     * 批量写数据
     */
    private static void writeToKafka() throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaBrokerList);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer = new KafkaProducer<String, String>(properties);

        for (int i = 1; i < 20000; i++) {
            Student student = new Student(i, "zhangsan" + i, "pwd" + i, 10 + i);
            ProducerRecord record = new ProducerRecord<String, String>(kafkaTopic, null, null, JSON.toJSONString(student));
            producer.send(record);
            System.out.println("发送数据: " + JSON.toJSONString(student));

            Thread.sleep(500);
        }

        producer.flush();
    }

    public static void main(String[] args) throws Exception {
        writeToKafka();
    }
}
