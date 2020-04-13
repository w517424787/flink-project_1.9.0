package com.sdf.flink.streaming;

import com.alibaba.fastjson.JSON;
import com.sdf.flink.model.Student;
import com.sdf.flink.sink.SinkToMySQL2;
import com.sdf.flink.util.GetKafkaProperties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class GetDataFromKafkaSinkToMySQL2 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //kafka属性
        Properties properties = GetKafkaProperties.getKafkaProperties();

        SingleOutputStreamOperator<Student> student = env.addSource(new FlinkKafkaConsumer<>("student",
                new SimpleStringSchema(), properties)).setParallelism(1)
                .map(line -> JSON.parseObject(line, Student.class));

        //批量插入数据到MySQL中，通过窗口函数来进行操作
        student.timeWindowAll(Time.minutes(1)).apply(new AllWindowFunction<Student, List<Student>, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<Student> values, Collector<List<Student>> out) throws Exception {
                ArrayList<Student> students = Lists.newArrayList(values);
                if (students.size() > 0) {
                    System.out.println("1分钟内收集到student的数据条数是：" + students.size());
                    out.collect(students);
                }
            }
        }).addSink(new SinkToMySQL2());

        env.execute("Sink To Mysql");
    }
}
