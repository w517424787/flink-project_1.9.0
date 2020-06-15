package com.sdf.flink.streaming;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Modify by wwg 2020-06-13
 * 简单的Flink CEP案例
 */

public class FlinkCEPDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<Tuple3<String, String, Long>> data = new ArrayList<>();
        data.add(Tuple3.of("Marry", "外套", 1L));
        data.add(Tuple3.of("Marry", "帽子", 1L));
        data.add(Tuple3.of("Marry", "帽子", 2L));
        data.add(Tuple3.of("Marry", "帽子", 3L));
        data.add(Tuple3.of("Ming", "衣服", 1L));
        data.add(Tuple3.of("Marry", "鞋子", 1L));
        data.add(Tuple3.of("Marry", "鞋子", 2L));
        data.add(Tuple3.of("LiLei", "帽子", 1L));
        data.add(Tuple3.of("LiLei", "帽子", 2L));
        data.add(Tuple3.of("LiLei", "帽子", 3L));

        DataStreamSource<Tuple3<String, String, Long>> dataStream = env.fromCollection(data);

        //定义Pattern
        Pattern<Tuple3<String, String, Long>, Tuple3<String, String, Long>> pattern = Pattern.<Tuple3<String, String, Long>>begin("start")
                .where(new SimpleCondition<Tuple3<String, String, Long>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, Long> value) throws Exception {
                        return value.f1.equals("帽子");
                    }
                }).next("middle").where(new SimpleCondition<Tuple3<String, String, Long>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, Long> value) throws Exception {
                        return value.f1.equals("帽子");
                    }
                });

        KeyedStream keyedStream = dataStream.keyBy(0);
        PatternStream patternStream = CEP.pattern(keyedStream, pattern);
        SingleOutputStreamOperator<String> result = patternStream.select(new PatternSelectFunction<Tuple3<String, String, Long>, String>() {

            @Override
            public String select(Map<String, List<Tuple3<String, String, Long>>> map) throws Exception {
                List<Tuple3<String, String, Long>> middle = map.get("middle");
                return middle.get(0).f0 + ":" + middle.get(0).f2 + ":" + "连续搜索两次帽子!";
            }
        });

        result.print();
        env.execute("execute cep");
    }
}
