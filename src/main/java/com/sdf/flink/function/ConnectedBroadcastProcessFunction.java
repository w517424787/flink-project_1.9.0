package com.sdf.flink.function;

import com.sdf.flink.model.Config;
import com.sdf.flink.model.EvaluatedResult;
import com.sdf.flink.model.UserEvent;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 定义广播函数
 */

public class ConnectedBroadcastProcessFunction extends KeyedBroadcastProcessFunction<Object, UserEvent, Config, EvaluatedResult> {
    @Override
    public void processElement(UserEvent value, ReadOnlyContext ctx, Collector<EvaluatedResult> out) throws Exception {

    }

    @Override
    public void processBroadcastElement(Config value, Context ctx, Collector<EvaluatedResult> out) throws Exception {

    }
}
