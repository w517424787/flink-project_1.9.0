package com.sdf.flink.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Modify by wwg 2020-04-22
 * 将数据插入到HBase中
 */

public class SinkToHBase extends RichSinkFunction<String> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {

    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
