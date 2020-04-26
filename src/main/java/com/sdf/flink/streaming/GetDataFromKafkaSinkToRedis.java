package com.sdf.flink.streaming;

import com.sdf.flink.util.KafkaConfigUtil;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

/**
 * Modify by wwg 2020-04-26
 * 将结果数据写入redis中
 */

public class GetDataFromKafkaSinkToRedis {
    public static void main(String[] args) throws Exception {
        final String broker_list = "192.168.7.113:9092,192.168.7.114:9092,192.168.7.115:9092";
        final String group_id = "group_topn";

        //加载redis
        FlinkJedisPoolConfig redisConfig = new FlinkJedisPoolConfig.Builder().setHost("192.168.7.115").setPort(6379).build();

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

        //数据插入到Redis中
        wcount.addSink(new RedisSink<>(redisConfig, new MyRedisSink())).setParallelism(1);

        env.execute("flink_redis_demo");
    }

    /**
     * 自定义redis sink
     */
    private static class MyRedisSink implements RedisMapper<Tuple2<String, Integer>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "flink-demo");
        }

        @Override
        public String getKeyFromData(Tuple2<String, Integer> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Integer> data) {
            return data.f1.toString();
        }
    }
}
