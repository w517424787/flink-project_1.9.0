package com.sdf.flink.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink Word Count程序
 */
public class WordCountDemo {

    private static final String[] WORDS = new String[]{
            "To be, or not to be,--that is the question:--",
            "Whether 'tis nobler in the mind to suffer"};

    public static void main(String[] args) throws Exception {

        //创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //接收参数，参数格式：--key1 value1 --key2 value2 --key3 value3
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));

        //先将数据根据分隔符进行拆分，并输出成（key,value）格式
        //注意：这里的Tuple2是org.apache.flink.api.java.tuple.Tuple2，不是scala中的Tuple2
        env.fromElements(WORDS).flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.toLowerCase().split("\\W+");
                for (String word : words) {
                    //将单个单词转换成（key，1）格式
                    if (word.length() > 0) {
                        out.collect(new Tuple2<>(word, 1));
                    }
                }
            }
        }).keyBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
            }
        }).print();

        env.execute("Flink Word Count Demo");
    }
}
