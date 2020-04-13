package com.sdf.flink.streaming;

import com.alibaba.fastjson.JSON;
import com.sdf.flink.model.Student;
import com.sdf.flink.sink.SinkToMySQL;
import com.sdf.flink.util.GetKafkaProperties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class GetDataFromKafkaSinkToMySQL {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //kafka属性
        Properties properties = GetKafkaProperties.getKafkaProperties();

        SingleOutputStreamOperator<Student> student = env.addSource(new FlinkKafkaConsumer<>("student",
                new SimpleStringSchema(), properties)).setParallelism(1)
                .map(line -> JSON.parseObject(line, Student.class));

        student.addSink(new SinkToMySQL());

        env.execute("Sink To Mysql");
    }
}
