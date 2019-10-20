package com.sdf.flink;

import com.alibaba.fastjson.JSON;
import com.sdf.flink.model.UserEvent;
import com.sdf.flink.util.ConvertDateUtils;

public class test {
    public static void main(String[] args) {
        //System.out.println(ConvertDateUtils.convertDateToLong("2019-10-20 09:20:10","yyyy-MM-dd HH:mm:ss"));

        String str = "{\"userId\":\"d8f3368aba5df27a39cbcfd36ce8084f\",\"channel\":\"APP\",\"eventType\":\"PURCHASE\"," +
                "\"eventTime\":\"2018-06-12 09:30:28\",\"data\":{\"productId\":196,\"price\":600.00,\"amount\":600.00}}";
        System.out.println(JSON.parseObject(str, UserEvent.class).getEventType());
    }
}
