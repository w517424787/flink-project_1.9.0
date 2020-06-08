package com.sdf.flink.streaming;

import com.sdf.flink.model.CityConfig;
import com.sdf.flink.source.CityDimFromMysql;
import com.sdf.flink.util.KafkaConfigUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;


/**
 * Modify by wwg 2020-06-04
 * 通过广播维度的方式来进行join操作
 * 通过市ID去转换市的名称
 * 如：cityID：5110，cityDesc：内江市
 */
public class FlinkStreamingDimBroadcastJoin {

    public static void main(String[] args) throws Exception {
        final String broker_list = "192.168.7.113:9092,192.168.7.114:9092,192.168.7.115:9092";
        final String group_id = "group_dim_test";
        final Logger LOG = LoggerFactory.getLogger(FlinkStreamingDimBroadcastJoin.class);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //维度数据
        DataStreamSource<CityConfig> dimStream = env.addSource(new CityDimFromMysql());
        final MapStateDescriptor<Integer, CityConfig> stateDescriptor = new MapStateDescriptor<>("dim_state", Types.INT, Types.POJO(CityConfig.class));
        BroadcastStream<CityConfig> broadcastStream = dimStream.broadcast(stateDescriptor);

        //读取数据
        //有界数据就会照成processElement先执行，processBroadcastElement后执行，照成无法读取到广播数据
        //Integer[] cityIds = {5103, 5104};
        //List<Integer> cityList = Arrays.asList(cityIds);
        //DataStream<Integer> dataStream = env.fromCollection(cityList);
        //通过读取kafka数据来模拟
        //kafka属性
        Properties properties = KafkaConfigUtil.buildConsumerProps(broker_list, group_id);

        @SuppressWarnings("unchecked")
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer("dim_test",
                new SimpleStringSchema(), properties);

        //设置offset读取位置
        consumer.setStartFromGroupOffsets(); //默认从group.id中读取

        SingleOutputStreamOperator<Integer> dataStreamSource = env.addSource(consumer).map(Integer::valueOf);
        //关联维度数据流
        SingleOutputStreamOperator<Tuple2<Integer, String>> result = dataStreamSource.connect(broadcastStream).process(new BroadcastProcessFunction<Integer, CityConfig, Tuple2<Integer, String>>() {
            final MapStateDescriptor<Integer, CityConfig> stateDescriptor = new MapStateDescriptor<>("dim_state", Types.INT, Types.POJO(CityConfig.class));
            @Override
            public void processElement(Integer value, ReadOnlyContext ctx, Collector<Tuple2<Integer, String>> out) throws Exception {
                ReadOnlyBroadcastState<Integer, CityConfig> readOnlyBroadcastState = ctx.getBroadcastState(stateDescriptor);
                CityConfig cityConfig = readOnlyBroadcastState.get(value);
                if (cityConfig != null && value.equals(cityConfig.getCityId())) {
                    out.collect(Tuple2.of(value, cityConfig.getCityDesc()));
                }
            }

            @Override
            public void processBroadcastElement(CityConfig value, Context ctx, Collector<Tuple2<Integer, String>> out) throws Exception {
                ctx.getBroadcastState(stateDescriptor).put(value.getCityId(), value);
            }
        });

        result.print();

//        List<Map<Integer, String>> list = new ArrayList<>();
//        Map<Integer, String> dimMap = new HashMap<>();
//        dimMap.put(5106, "德阳市");
//        dimMap.put(5101, "成都市");
//        dimMap.put(5103, "自贡市");
//        dimMap.put(5104, "攀枝花市");
//        dimMap.put(5105, "泸州市");
//        list.add(dimMap);
//
//        //维度数据流
//        DataStreamSource<Map<Integer, String>> dimStream = env.fromCollection(list);
//
//        //定义广播
//        final MapStateDescriptor<String, Map<Integer, String>> stateDescriptor =
//                new MapStateDescriptor<>("dim_state", BasicTypeInfo.STRING_TYPE_INFO, new MapTypeInfo<>(Integer.class, String.class));
//        //转换成广播流
//        BroadcastStream<Map<Integer, String>> broadcastStream = dimStream.setParallelism(1).broadcast(stateDescriptor);
//
//        //读取数据
//        Integer[] cityIds = {5103, 5104};
//        List<Integer> cityList = Arrays.asList(cityIds);
//        DataStream<Integer> dataStream = env.fromCollection(cityList);
//
//        //关联维度数据流
//        SingleOutputStreamOperator<Tuple2<Integer, String>> result = dataStream.connect(broadcastStream).process(new BroadcastProcessFunction<Integer,
//                Map<Integer, String>, Tuple2<Integer, String>>() {
//
//            List<Map<Integer, String>> list = new ArrayList<>();
//            private final MapStateDescriptor<String, Map<Integer, String>> stateDescriptor =
//                    new MapStateDescriptor<>("dim_state", BasicTypeInfo.STRING_TYPE_INFO, new MapTypeInfo<>(Integer.class, String.class));
//
//            @Override
//            public void processBroadcastElement(Map<Integer, String> value, Context ctx, Collector<Tuple2<Integer, String>> out) throws Exception {
//                for (Integer key : value.keySet()) {
//                    ctx.getBroadcastState(stateDescriptor).put(key.toString(), value);
//                }
//            }
//
//            @Override
//            public void processElement(Integer value, ReadOnlyContext ctx, Collector<Tuple2<Integer, String>> out) throws Exception {
//
//            }
//        });
//
//        result.print();

        env.execute("Flink Dim Join");
    }
}