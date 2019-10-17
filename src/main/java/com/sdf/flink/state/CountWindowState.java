package com.sdf.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class CountWindowState {
    public static class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

        //定义状态值，第一个字段为值个数，第二个字段为值总和
        private transient ValueState<Tuple2<Long, Long>> sum;

        @Override
        public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {
            //当前状态值
            Tuple2<Long, Long> currentSum = sum.value();

            //没有设定默认值时，defaultValue为null,需要判断给个初始值
            if (currentSum == null) {
                currentSum = Tuple2.of(0L, 0L);
            }

            //添加上新进的值
            currentSum.f0 += 1;
            currentSum.f1 += input.f1;

            //更新状态
            sum.update(currentSum);

            //输出当前值
            if (currentSum.f0 >= 2) {
                out.collect(new Tuple2<>(input.f0, currentSum.f1 / currentSum.f0));
                //清除状态
                sum.clear();
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<>("average",
                    TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                    }));//, Tuple2.of(0L, 0L)

            sum = getRuntimeContext().getState(descriptor);
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L),
                Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
                .keyBy(0)
                .flatMap(new CountWindowAverage())
                .print();

        env.execute("state demo");
    }
}
