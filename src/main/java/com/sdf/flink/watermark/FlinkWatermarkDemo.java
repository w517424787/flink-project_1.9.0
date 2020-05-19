package com.sdf.flink.watermark;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

/**
 * Flink Watermark测试
 * 通过读取socket端口来测试数据延迟与水印关系
 * 数据格式：key,timestamp，如：hello,1589795445000
 */

public class FlinkWatermarkDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设定时间类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        //读取数据
        DataStream<String> dstream = env.socketTextStream("192.168.7.115", 9999)
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {

                    //当前最大的timestamp
                    long currentMaxTimestamp = 0L;
                    //当前的watermark
                    long currentWatermark = 0L;
                    //最大延迟时间，这里设置(5s)
                    long maxDelayAllowed = 5000L;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        currentWatermark = currentMaxTimestamp - maxDelayAllowed;
                        return new Watermark(currentWatermark);
                    }

                    @Override
                    public long extractTimestamp(String element, long previousElementTimestamp) {
                        String[] arrays = element.split(",");
                        long timeStamp = Long.parseLong(arrays[1]);
                        currentMaxTimestamp = Math.max(timeStamp, currentMaxTimestamp);
                        System.out.println("Key:" + arrays[0] + ",EventTime:" + timeStamp + ",Watermark:" + currentWatermark);
                        return timeStamp;
                    }
                });

        dstream.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return new Tuple2<>(value.split(",")[0], value.split(",")[1]);
            }
        }).keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .fold("Start:", new FoldFunction<Tuple2<String, String>, String>() {
                    @Override
                    public String fold(String str, Tuple2<String, String> value) throws Exception {
                        return str + "-" + value.f1;
                    }
                }).print();

        env.execute("Flink_Watermark_Demo");
    }
}
