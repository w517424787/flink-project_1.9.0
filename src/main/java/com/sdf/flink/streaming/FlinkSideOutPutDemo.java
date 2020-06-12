package com.sdf.flink.streaming;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;

/**
 * Modify by wwg 2020-06-12
 * 流拆分操作
 */

public class FlinkSideOutPutDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Tuple3<Integer, Integer, Integer>> data = new ArrayList<>();
        data.add(new Tuple3<>(0, 1, 0));
        data.add(new Tuple3<>(0, 1, 1));
        data.add(new Tuple3<>(0, 2, 2));
        data.add(new Tuple3<>(0, 1, 3));
        data.add(new Tuple3<>(1, 2, 5));
        data.add(new Tuple3<>(1, 2, 9));
        data.add(new Tuple3<>(1, 2, 11));
        data.add(new Tuple3<>(1, 2, 13));

        //数据集
        DataStreamSource<Tuple3<Integer, Integer, Integer>> dataStream = env.fromCollection(data);
        //定义拆分流情况
        final OutputTag<Tuple3<Integer, Integer, Integer>> zeroStream = new OutputTag<Tuple3<Integer, Integer, Integer>>("zeroStream") {
        };
        final OutputTag<Tuple3<Integer, Integer, Integer>> oneStream = new OutputTag<Tuple3<Integer, Integer, Integer>>("oneStream") {
        };

        SingleOutputStreamOperator<Tuple3<Integer, Integer, Integer>> result = dataStream.process(new ProcessFunction<Tuple3<Integer, Integer, Integer>,
                Tuple3<Integer, Integer, Integer>>() {
            @Override
            public void processElement(Tuple3<Integer, Integer, Integer> value, Context ctx,
                                       Collector<Tuple3<Integer, Integer, Integer>> out) throws Exception {
                if (value.f0 == 0) {
                    ctx.output(zeroStream, value);
                } else if (value.f0 == 1) {
                    ctx.output(oneStream, value);
                }
            }
        });

        //输出流
        result.getSideOutput(zeroStream).print();
        //result.getSideOutput(oneStream).print();

        //打印结果
        String jobName = "user defined streaming source";
        env.execute(jobName);
    }
}
