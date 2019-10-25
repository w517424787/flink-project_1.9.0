package com.sdf.flink.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sdf.flink.model.UserEvent;
import com.sdf.flink.util.ConvertDateUtils;
import sun.security.action.GetPropertyAction;

import java.io.File;
import java.security.AccessController;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

public class TmpDirTest {
    public static void main(String[] args) {
        //System.out.println(ConvertDateUtils.convertDateToLong("2019-10-20 09:20:10","yyyy-MM-dd HH:mm:ss"));

//        String str = "{\"userId\":\"d8f3368aba5df27a39cbcfd36ce8084f\",\"channel\":\"APP\",\"eventType\":\"PURCHASE\"," +
//                "\"eventTime\":\"2018-06-12 09:30:28\",\"data\":{\"productId\":196,\"price\":600.00,\"amount\":600.00}}";
//        System.out.println(JSON.parseObject(str, UserEvent.class).getEventType());

//        System.out.println(Runtime.getRuntime().maxMemory());
//        final SecureRandom random = new SecureRandom();
//        System.out.println(random.nextLong());
        //final File tmpdir = new File(AccessController.doPrivileged(new GetPropertyAction("java.io.tmpdir")));
        //System.out.println(tmpdir);
//        System.out.println(Runtime.getRuntime().maxMemory() / (1024 * 1024 * 1024));
//        Map<String, Integer> map = new HashMap<>();
//        map.put("a", 1);
//        map.put("b", 2);
//        System.out.println("eventTypeCounts:" + map);

        JSONObject json = JSON.parseObject("{\"userId\":\"11\",\"itemId\":\"0001\"}");
        System.out.println(json);
    }
}
