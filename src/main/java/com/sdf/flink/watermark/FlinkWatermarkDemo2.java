package com.sdf.flink.watermark;

import com.sdf.flink.model.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * Flink Watermark使用介绍
 * 数据格式：key,timestamp,temperature，如：sensor-1, 1589795445000,20.0，sensor-1, 1589795446000,25.0
 */

public class FlinkWatermarkDemo2 {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //定义窗口关闭后，延迟数据存储集合
        OutputTag outputTag = new OutputTag<SensorReading>("side");
        DataStream<String> dStream = env.socketTextStream("192.168.7.115", 9999);

        DataStream<SensorReading> dataStream = dStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] arrays = value.split(",");
                return new SensorReading(arrays[0].trim(), Long.parseLong(arrays[1].trim()),
                        Double.parseDouble(arrays[2].trim()));
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(5)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                //获取事件的时间戳
                return element.getTimestamp();
            }
        });

        //获取窗口中最小的温度值
        @SuppressWarnings("unchecked")
        DataStream<SensorReading> minDStream = dataStream.keyBy(value -> value.getId())
                //.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .allowedLateness(Time.seconds(5)).sideOutputLateData(outputTag).minBy("temperature");


        dataStream.print("data");
        minDStream.print("min");
        ((SingleOutputStreamOperator<SensorReading>) minDStream).getSideOutput(outputTag).print("side");

        env.execute("FlinkWatermarkDemo2");
    }
}
