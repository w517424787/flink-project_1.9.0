package com.sdf.flink.batch;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class BatchDemoBroadcast {
    public static void main(String[] args) throws Exception {

        //获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //构造广播数据
        ArrayList<Tuple2<String, Integer>> broadData = new ArrayList<>();
        broadData.add(new Tuple2<>("Tom", 30));
        broadData.add(new Tuple2<>("Jason", 25));
        broadData.add(new Tuple2<>("Jack", 28));

        //转换成DataSet
        DataSet<Tuple2<String, Integer>> dataSet = env.fromCollection(broadData);

        //构造测试数据
        DataSet<String> data = env.fromElements("Tom", "Jason", "Jack");

        //注意：在这里需要使用到RichMapFunction获取广播变量
        DataSet<String> result = data.map(new RichMapFunction<String, String>() {

            List<Tuple2<String, Integer>> broadDataList = new ArrayList<>();
            HashMap<String, Integer> hashMap = new HashMap<>();

            //获取广播变量
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //获取广播数值
                this.broadDataList = getRuntimeContext().getBroadcastVariable("broadCastMapName");
                for (Tuple2<String, Integer> tuple : broadDataList) {
                    hashMap.put(tuple.f0, tuple.f1);
                }
            }

            @Override
            public String map(String value) {
                return value + "," + hashMap.getOrDefault(value, 0);
            }

        }).withBroadcastSet(dataSet, "broadCastMapName"); //广播数据

        result.print();
    }
}
