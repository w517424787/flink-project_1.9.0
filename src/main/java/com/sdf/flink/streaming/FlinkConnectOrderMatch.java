package com.sdf.flink.streaming;

import com.alibaba.fastjson.JSON;
import com.sdf.flink.model.Order;
import com.sdf.flink.util.KafkaConfigUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Objects;

public class FlinkConnectOrderMatch {

    //定义输出未关联到的流
    private static OutputTag<Order> bigOrderTag = new OutputTag<>("bigOrder");
    private static OutputTag<Order> smallOrderTag = new OutputTag<>("smallOrder");

    public static void main(String[] args) throws Exception {
        final String broker_list = "192.168.7.113:9092,192.168.7.114:9092,192.168.7.115:9092";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(2);

        //读取大订单流数据
        FlinkKafkaConsumerBase<String> bigOrder = new FlinkKafkaConsumer<>("big_order_topic",
                new SimpleStringSchema(),
                KafkaConfigUtil.buildConsumerProps(broker_list, "group_big_order_topic"))
                .setStartFromGroupOffsets();

        //将JSON数据转换成Order类型
        KeyedStream<Order, String> bigOrderStream = env.addSource(bigOrder).uid("big_order_topic")
                .filter(Objects::nonNull)
                .map(value -> JSON.parseObject(value, Order.class))
                //提取EventTime，分配Watermark，设置最大延迟时间
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(60)) {
                    @Override
                    public long extractTimestamp(Order order) {
                        return order.getTime();
                    }
                }).keyBy(Order::getOrderId);

        //读取小订单流数据
        FlinkKafkaConsumerBase<String> smallOrder = new FlinkKafkaConsumer<>("small_order_topic",
                new SimpleStringSchema(),
                KafkaConfigUtil.buildConsumerProps(broker_list, "group_small_order_topic"))
                .setStartFromGroupOffsets();

        //将JSON数据转换成Order类型
        KeyedStream<Order, String> smallOrderStream = env.addSource(bigOrder).uid("small_order_topic")
                .filter(Objects::nonNull)
                .map(value -> JSON.parseObject(value, Order.class))
                //提取EventTime，分配Watermark，设置最大延迟时间
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(60)) {
                    @Override
                    public long extractTimestamp(Order order) {
                        return order.getTime();
                    }
                }).keyBy(Order::getOrderId);

        //使用connect连接大小订单的流，然后使用CoProcessFunction进行数据匹配
        SingleOutputStreamOperator<Tuple2<Order, Order>> connStream = bigOrderStream.connect(smallOrderStream)
                .process(new CoProcessFunction<Order, Order, Tuple2<Order, Order>>() {

                    //存储大订单先到来的数据
                    ValueState<Order> bigState;
                    //存储小订单先到来的数据
                    ValueState<Order> smallState;
                    //当前注册的定时器时间戳
                    ValueState<Long> timerState;

                    @Override
                    public void processElement1(Order bigOrder, Context ctx, Collector<Tuple2<Order, Order>> out) throws Exception {
                        //读取小订单数据
                        Order smallOrder = smallState.value();
                        //smallOrder不为空表示小订单先来了，直接将大小订单拼接发送到下游
                        if (smallOrder != null) {
                            out.collect(Tuple2.of(smallOrder, bigOrder));
                            //清空小订单对应的State信息
                            smallState.clear();
                            //这里可以将Timer清除。因为两个流都到了，没必要再触发onTimer了
                            ctx.timerService().deleteEventTimeTimer(timerState.value());
                            timerState.clear();

                        } else {
                            //小订单数据还未到，需要将大订单数据存储到状态中，在设定时间触发器，1分钟时间
                            bigState.update(bigOrder);
                            long time = bigOrder.getTime() + 60000;
                            timerState.update(time);
                            ctx.timerService().registerEventTimeTimer(time);
                        }
                    }

                    @Override
                    public void processElement2(Order smallOrder, Context ctx, Collector<Tuple2<Order, Order>> out) throws Exception {
                        //读取大订单数据
                        Order bigOrder = bigState.value();
                        //bigOrder不为空表示大订单先来了，直接将大小订单拼接发送到下游
                        if (bigOrder != null) {
                            out.collect(Tuple2.of(smallOrder, bigOrder));
                            //清空大订单对应的State信息
                            bigState.clear();
                            //这里可以将Timer清除。因为两个流都到了，没必要再触发onTimer了
                            ctx.timerService().deleteEventTimeTimer(timerState.value());
                            timerState.clear();

                        } else {
                            //大订单数据还未到，需要将小订单数据存储到状态中，在设定时间触发器，1分钟时间
                            smallState.update(smallOrder);
                            long time = smallOrder.getTime() + 60000;
                            timerState.update(time);
                            ctx.timerService().registerEventTimeTimer(time);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Order, Order>> out) throws Exception {
                        //未匹配到的大订单数据，输出大订单数据
                        if (bigState.value() != null) {
                            ctx.output(bigOrderTag, bigState.value());
                        }
                        //未匹配到的小订单数据，输出小订单数据
                        if (smallState.value() != null) {
                            ctx.output(smallOrderTag, smallState.value());
                        }
                        //清除状态
                        bigState.clear();
                        smallState.clear();
                        timerState.clear();
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        bigState = getRuntimeContext().getState(new ValueStateDescriptor<>("bigState", Order.class));
                        smallState = getRuntimeContext().getState(new ValueStateDescriptor<>("smallState", Order.class));
                        timerState = getRuntimeContext().getState(new ValueStateDescriptor<>("timerState", Long.class));
                    }
                });

        //输出正常关联到的数据
        connStream.print();
        //输出未关联到的大订单数据
        connStream.getSideOutput(bigOrderTag).print();
        //输出未关联到的小订单数据
        connStream.getSideOutput(smallOrderTag).print();

        env.execute("Flink_Connect_Demo");
    }
}
