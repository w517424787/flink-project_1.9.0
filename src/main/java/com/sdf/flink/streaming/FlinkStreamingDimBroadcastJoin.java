package com.sdf.flink.streaming;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.runtime.state.HeapBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/**
 * Modify by wwg 2020-06-04
 * 通过广播维度的方式来进行join操作
 * 通过市ID去转换市的名称
 * 如：cityID：5110，cityDesc：内江市
 */
public class FlinkStreamingDimBroadcastJoin {

    public static void main(String[] args) throws Exception {
        final Logger LOG = LoggerFactory.getLogger(FlinkStreamingDimBroadcastJoin.class);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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