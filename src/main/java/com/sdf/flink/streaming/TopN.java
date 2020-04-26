package com.sdf.flink.streaming;

import com.sdf.flink.util.KafkaConfigUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

/**
 * 每30s统计过去1min中销售Top3书籍
 */

public class TopN {
    public static void main(String[] args) throws Exception {

        final String broker_list = "192.168.7.113:9092,192.168.7.114:9092,192.168.7.115:9092";
        final String group_id = "group_topn";
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        //读取数据
        FlinkKafkaConsumer<String> input = new FlinkKafkaConsumer<>("topn", new SimpleStringSchema(),
                KafkaConfigUtil.buildConsumerProps(broker_list, group_id));
        input.setStartFromGroupOffsets();

        DataStream<String> stream = env.addSource(input);
        DataStream<Tuple2<String, Integer>> ds = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(new Tuple2<>(value, 1));
            }
        });

        //分组统计，并每30s统计过去1min中的数据，进行窗口滑动计算
        DataStream<Tuple2<String, Integer>> wcount = ds.keyBy(0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(60), Time.seconds(30)))
                .sum(1);

        //对滑动窗口中产生的数据进行TopN操作，滚动窗口时间和滑动窗口保持一致
        wcount.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .process(new TopNAllFunction(3)).print();

        env.execute("topN");
    }

    //TopN计算
    private static class TopNAllFunction extends ProcessAllWindowFunction<Tuple2<String, Integer>, String, TimeWindow> {

        private int topnSize = 3;

        public TopNAllFunction(int topnSize) {
            this.topnSize = topnSize;
        }

        @Override
        public void process(Context context, Iterable<Tuple2<String, Integer>> input, Collector<String> out) throws Exception {
            //将点击的次数当key，并进行比较
            TreeMap<Integer, Tuple2<String, Integer>> treeMap = new TreeMap<Integer, Tuple2<String, Integer>>(
                    new Comparator<Integer>() {
                        @Override
                        public int compare(Integer x, Integer y) {
                            if (x.equals(y)) {
                                return 0;
                            } else if (x < y) {
                                return 1;
                            } else {
                                return -1;
                            }
                        }
                    }
            );

            //只取TopN数据
            for (Tuple2<String, Integer> value : input) {
                treeMap.put(value.f1, value);
                //保留前几条数据，pollLastEntry是移除最后一条数据
                if (treeMap.size() > topnSize) { //只保留前面TopN个元素
                    treeMap.pollLastEntry();
                }
            }

            //输出数据
            out.collect("===============\n热销图书列表:\n");
            out.collect(new Timestamp(System.currentTimeMillis()) + ": " + treeMap.values().toString() + "\n");
            out.collect("===============");
        }
    }
}
