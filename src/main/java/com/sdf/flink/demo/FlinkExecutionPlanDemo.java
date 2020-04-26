package com.sdf.flink.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlinkExecutionPlanDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String path = "E:\\flink-demo\\flink-project_1.10.0\\data\\test.txt";

        DataStream<Tuple2<String, Integer>> dataStream =
                env.readTextFile(path).flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] values = line.split(",");
                        for (String word : values) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                }).keyBy(0).sum(1);

        dataStream.print();

        System.out.println(env.getExecutionPlan());

        //env.execute("demo");
    }
}
