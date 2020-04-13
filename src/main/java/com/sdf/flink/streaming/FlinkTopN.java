package com.sdf.flink.streaming;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sdf.flink.model.UserBrowseItemIdCount;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

/**
 * 实时计算热销商品TopN，监控用户配置的商品，判断热销程度
 * 商品配置可以通过Mysql或者Kafka Topic，这里简化就用本地文件进行模拟
 * 浏览商品数据格式：{"userId":"11","itemId":"10001"}，{"userId":"11","itemId":"10002"}，{"userId":"12","itemId":"10001"}
 */

public class FlinkTopN {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkTopN.class);

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        //获取商品配置表，监控热销商品
        //也可以通过读取Mysql数据库或者是Kafka Topic来获取监控商品
        URL url = FlinkTopN.class.getClassLoader().getResource("itemId.txt");
        String filePath = "";
        if (url != null) {
            filePath = url.getPath();
        }
        DataStream<String> configDS = env.readTextFile(filePath);
        configDS.print();

        //将监控商品进行广播
        MapStateDescriptor<String, String> configStateDescriptor = new MapStateDescriptor<>("itemId_descriptor",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<String>() {
                }));
        BroadcastStream<String> broadcastStream = configDS.broadcast(configStateDescriptor);

        //读取用户浏览商品流数据
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.7.113:9092,192.168.7.114:9092,192.168.7.115:9092");
        properties.put("zookeeper.connect", "192.168.7.110:2181,192.168.7.111:2181,192.168.7.112:2181");
        properties.put("group.id", "flink-topn");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "latest");

        FlinkKafkaConsumer userBrowseEvent = new FlinkKafkaConsumer<>("flink-topn", new SimpleStringSchema(), properties);

        //设置水位
        @SuppressWarnings("unchecked")
        DataStream<String> userBrowseDS = env.addSource(userBrowseEvent)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor() {
                    @Override
                    public long extractAscendingTimestamp(Object o) {
                        return System.currentTimeMillis();
                    }
                });

        //将用户浏览商品数据流和配置数据进行关联，过滤掉不需要监控的商品
        DataStream<Tuple2<String, Integer>> result = userBrowseDS.connect(broadcastStream)
                .process(new BroadcastProcessFunction<String, String, Tuple2<String, Integer>>() {
                    @Override
                    public void processElement(String value, ReadOnlyContext readOnlyContext, Collector<Tuple2<String, Integer>> out) throws Exception {
                        ReadOnlyBroadcastState<String, String> state = readOnlyContext.getBroadcastState(configStateDescriptor);

                        //判断用户浏览的商品是否在监控的商品中
                        //用户浏览商品数据格式：{"userId":"11","itemId":"10001"}
                        try {
                            JSONObject jsonObject = JSON.parseObject(value);
                            if (state.contains(jsonObject.getString("itemId"))) {
                                out.collect(new Tuple2<>(jsonObject.getString("itemId"), 1));
                            }
                        } catch (Exception e) {
                            LOG.info(e.getMessage());
                        }
                    }

                    @Override
                    @SuppressWarnings("unchecked")
                    public void processBroadcastElement(String value, Context context, Collector<Tuple2<String, Integer>> out) throws Exception {
                        BroadcastState<String, String> state = context.getBroadcastState(configStateDescriptor);

                        //更新状态
                        state.put(value, value);
                    }
                });

        //打印效果
        result.print();

        //按itemId进行分组统计，取TopN
        //每30秒统计下过去1分钟内商品浏览TopN，通过滑动窗口计算
        result.keyBy(0).timeWindow(Time.minutes(1), Time.seconds(30))
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                }, new WindowFunction<Integer, UserBrowseItemIdCount, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple key, TimeWindow timeWindow, Iterable<Integer> input,
                                      Collector<UserBrowseItemIdCount> out) throws Exception {
                        String itemId = key.getField(0);
                        Integer count = input.iterator().next();
                        out.collect(new UserBrowseItemIdCount(itemId, count, timeWindow.getEnd()));
                    }
                }).keyBy(UserBrowseItemIdCount::getTimeWindow)
                .process(new TopNHotItems(3))
                .print();

        env.execute("FlinkTopN");
    }
}

class TopNHotItems extends KeyedProcessFunction<Long, UserBrowseItemIdCount, String> {
    private final int topN;

    public TopNHotItems(int topN) {
        this.topN = topN;
    }

    //用于存储用户订单数量状态，待收齐同一个窗口的数据后，再触发TopN计算
    private ListState<UserBrowseItemIdCount> itemIdCountListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        //进行状态注册
        ListStateDescriptor<UserBrowseItemIdCount> itemIdStateDesc = new ListStateDescriptor<>("itemIdState", UserBrowseItemIdCount.class);
        itemIdCountListState = getRuntimeContext().getListState(itemIdStateDesc);
    }

    @Override
    public void processElement(UserBrowseItemIdCount userBrowseItemIdCount, Context context, Collector<String> collector) throws Exception {
        //每条数据都保存到状态中
        itemIdCountListState.add(userBrowseItemIdCount);

        //注册windowEnd+1的EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数
        context.timerService().registerProcessingTimeTimer(userBrowseItemIdCount.getTimeWindow() + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);

        //获取所有商品的订单信息
        List<UserBrowseItemIdCount> allUserOrder = new ArrayList<>();
        for (UserBrowseItemIdCount itemIdCount : itemIdCountListState.get()) {
            allUserOrder.add(itemIdCount);
        }

        //提前清除状态中的数据，释放空间
        itemIdCountListState.clear();

        //按订单量从大到小进行排序
        allUserOrder.sort(new Comparator<UserBrowseItemIdCount>() {
            @Override
            public int compare(UserBrowseItemIdCount o1, UserBrowseItemIdCount o2) {
                return o2.getOrderCount() - o1.getOrderCount();
            }
        });

        //返回指定TopN数据
        for (int i = 0; i < topN; i++) {
            if (i < allUserOrder.size()) {
                UserBrowseItemIdCount userBrowseItemIdCount = allUserOrder.get(i);
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("itemId:", userBrowseItemIdCount.getItemId());
                jsonObject.put("orderQt:", userBrowseItemIdCount.getOrderCount());
                out.collect(jsonObject.toString());
            }
        }
    }
}
