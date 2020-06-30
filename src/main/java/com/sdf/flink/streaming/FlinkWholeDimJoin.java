package com.sdf.flink.streaming;

import com.sdf.flink.function.WholeLoad;
import com.sdf.flink.model.CityConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

public class FlinkWholeDimJoin {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取数据
        String[] cityIds = {"{cityId:5101}", "{cityId:5103}"};
        List<String> cityList = Arrays.asList(cityIds);
        DataStream<String> dataStream = env.fromCollection(cityList);

        SingleOutputStreamOperator<CityConfig> result = dataStream.map(new WholeLoad());
        result.print();
        env.execute("Flink Dim Join");
    }
}
