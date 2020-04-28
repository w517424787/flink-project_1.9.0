package com.sdf.flink.hbase;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class GetJSONDataIntoHBase {

    private static final Logger LOG = LoggerFactory.getLogger(GetJSONDataIntoHBase.class);

    /**
     * 读取客户端埋点数据，将其插入到HBase表中
     * Modify by wwg 2020-04-28
     * 数据格式：
     * {"appId":9999,"id":17012285,"playerId":1000001,"collectTime":"2019-12-10 14:28:08",
     * "tableName":"dwm.m_f_game_buried_point_stat","clickType":"20001.3.1.1.7",
     * "devicecode":"DESKTOP-OIH0F6Nxaasa","serverVersion":"TYF","os":4,"param":"2002.1"}
     * Map<String, Map<String, String>> dataMap
     * (cf,((column,value),(column,value),(column,value)))
     *
     * @param jsonStr
     * @param admin
     * @param conn
     */
    public static void getBuriedDataToHBase(String jsonStr, Admin admin, Connection conn) {
        if (jsonStr != null && !"".equals(jsonStr)) {
            try {
                //把json中字段全部转成小写
                JSONObject jsonObject = JSON.parseObject(jsonStr.toLowerCase());
                String tableName = jsonObject.getOrDefault("tablename", "").toString();
                //判断表字段是否为空
                if ("".equals(tableName)) {
                    LOG.info("tableName字段不存在!");
                } else {
                    String columnFamily = "info";
                    Map<String, Map<String, String>> dataMap = new HashMap<>();
                    Map<String, String> valueMap = new HashMap<>();
                    Set<String> keys = jsonObject.keySet();
                    for (String key : keys) {
                        //不需要插入tableName
                        if (!key.equals("tablename")) {
                            valueMap.put(key.toLowerCase(), jsonObject.getString(key));
                        }
                    }
                    dataMap.put(columnFamily, valueMap);

                    //HBase rowKey格式：playerid_collectTime_clickType
                    //collectTime格式转换成：yyyyMMddHHmmss，需要先转成Date格式，在进行转换
                    String playerId = jsonObject.getString("playerid");
                    String collectTime = jsonObject.getString("collecttime");
                    String clickType = jsonObject.getString("clicktype");
                    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Date date = dateFormat.parse(collectTime);
                    String time = new SimpleDateFormat("yyyyMMddHHmmss").format(date);
                    String rowKey = playerId + "_" + time + "_" + clickType;

                    //插入数据到HBase中
                    HBaseClient.addData(admin, conn, tableName, rowKey, dataMap);
                }
            } catch (Exception e) {
                LOG.info("json格式不正确:" + jsonStr);
            }
        } else {
            LOG.info("json can not be null or empty!");
        }
    }
}
