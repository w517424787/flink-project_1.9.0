package com.sdf.flink.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class FlinkSinkDemo1 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        //TableEnvironment tEnv = TableEnvironment.create(settings);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        DataStream<String> ds = env.fromElements("a", "b", "c");
    }
}
