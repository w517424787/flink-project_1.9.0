package com.sdf.flink.source;

import com.alibaba.fastjson.JSON;
import com.sdf.flink.model.FlinkPVData;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Random;

public class FlinkPVSource extends RichSourceFunction<String> {

    private boolean isRunning = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        //模拟用户编号
        String[] userIds = {"A10001", "A10002", "A10003", "A10004", "A10005"};
        String[] eventTypes = {"click", "buy", "pay", "add", "move"};
        FlinkPVData pvData = new FlinkPVData();

        int index;
        while (isRunning) {
            index = new Random().nextInt(5);
            pvData.setEventType(eventTypes[index]);
            index = new Random().nextInt(5);
            pvData.setUserId(userIds[index]);
            pvData.setPlatform("web");
            pvData.setTs(System.currentTimeMillis());

            //转换成JSON格式
            ctx.collect(JSON.toJSONString(pvData));

            //模拟每1秒中产生一条数据
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
