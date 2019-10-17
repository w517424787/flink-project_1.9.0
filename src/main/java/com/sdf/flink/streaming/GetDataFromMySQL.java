package com.sdf.flink.streaming;

import com.sdf.flink.source.SourceFromMySQL;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

public class GetDataFromMySQL {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.addSource(new SourceFromMySQL()).print();

        //通过直接调用sink来输出数据
        env.addSource(new SourceFromMySQL()).addSink(new PrintSinkFunction<>());

        env.execute("Flink mysql data source");
    }
}
