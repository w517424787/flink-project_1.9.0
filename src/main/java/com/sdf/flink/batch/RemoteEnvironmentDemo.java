package com.sdf.flink.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * 本地提交任务到远程集群进行测试
 */
public class RemoteEnvironmentDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("192.168.7.112", 8081);
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());
        DataSource<String> text = env.readTextFile("hdfs://192.168.7.112:8020/flink/data");
        text.writeAsText("hdfs://192.168.7.112:8020/flink/result/test.txt", FileSystem.WriteMode.OVERWRITE);
//        text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
//                //数据格式：47411,654323,6543,福海县,3
//                String[] words = value.split(",");
//                if (words.length > 2) {
//                    out.collect(new Tuple2<>(words[2], 1));
//                }
//            }
//        }).groupBy(0)
//                .sum(1)
//                .writeAsText("hdfs://192.168.7.112:8020/flink/result", FileSystem.WriteMode.OVERWRITE);

        env.execute();
    }
}
