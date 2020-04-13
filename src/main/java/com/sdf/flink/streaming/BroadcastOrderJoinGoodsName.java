package com.sdf.flink.streaming;

import com.alibaba.fastjson.JSONObject;
import com.sdf.flink.model.Goods;
import com.sdf.flink.model.Order;
import com.sdf.flink.util.KafkaConfigUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.util.Collector;

import java.util.Objects;

public class BroadcastOrderJoinGoodsName {
    public static void main(String[] args) throws Exception {

        final String broker_list = "192.168.7.113:9092,192.168.7.114:9092,192.168.7.115:9092";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        //读取订单数据
        FlinkKafkaConsumerBase<String> consumerOrder = new FlinkKafkaConsumer<>("order_topic",
                new SimpleStringSchema(),
                KafkaConfigUtil.buildConsumerProps(broker_list, "group_order_topic"))
                .setStartFromGroupOffsets();

        //将订单数据从json解析成Order类型
        SingleOutputStreamOperator<Order> orderStream = env.addSource(consumerOrder)
                .uid("order_topic")
                .filter(Objects::nonNull)
                .map(value -> JSONObject.parseObject(value, Order.class));


        //读取商品Id数据
        FlinkKafkaConsumerBase<String> consumerGoods = new FlinkKafkaConsumer<>("goods_dim_topic",
                new SimpleStringSchema(),
                KafkaConfigUtil.buildConsumerProps(broker_list, "group_goods_dim_topic"))
                .setStartFromEarliest(); //每次启动时，商品维度属性都要全部加载到状态中
                //.setStartFromGroupOffsets();

        //将商品Id和商品名称的映射关系从json解析成Goods类
        SingleOutputStreamOperator<Goods> goodsStream = env.addSource(consumerGoods)
                .uid("goods_dim_topic")
                .filter(Objects::nonNull)
                .map(value -> JSONObject.parseObject(value, Goods.class));

        //存储Goods维度信息的MapState
        final MapStateDescriptor<Integer, String> GOODS_STATE =
                new MapStateDescriptor<>("GOODS_STATE", BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);

        //Order流与Goods流进行connect
        SingleOutputStreamOperator<Tuple2<Order, String>> connStream = orderStream.connect(goodsStream.broadcast(GOODS_STATE))
                .process(new BroadcastProcessFunction<Order, Goods, Tuple2<Order, String>>() {
                    @Override
                    public void processElement(Order order, ReadOnlyContext ctx, Collector<Tuple2<Order, String>> out) throws Exception {
                        ReadOnlyBroadcastState<Integer, String> broadcastState = ctx.getBroadcastState(GOODS_STATE);
                        //从状态中获取商品名称，并发送到下游
                        String goodsName = broadcastState.get(order.getGoodsId());
                        out.collect(Tuple2.of(order, goodsName));
                    }

                    @Override
                    public void processBroadcastElement(Goods goods, Context ctx, Collector<Tuple2<Order, String>> out) throws Exception {
                        //更新商品的维度信息到状态中
                        BroadcastState<Integer, String> broadcastState = ctx.getBroadcastState(GOODS_STATE);
                        //判断商品是否下架
                        if (goods.isRemoved()) {
                            //商品下架了，应该从状态中移除，减少状态的大小
                            broadcastState.remove(goods.getGoodsId());
                        } else {
                            // 商品上架，应该添加到状态中，用于关联商品信息
                            broadcastState.put(goods.getGoodsId(), goods.getGoodsName());
                        }
                    }
                }).setParallelism(1);

        //输出结果
        connStream.map(value -> "{" + value.f0.getTime().toString()
                + "," + value.f0.getOrderId() + "," + value.f0.getUserId() + ","
                + value.f0.getGoodsId() + "," + value.f1 + "}").print();

        //执行Job
        env.execute("Order_Goods_Demo");
    }
}
