package com.sdf.flink.model;

import java.io.Serializable;

/**
 * 模拟购物点击数据，计算PV、UV，简化数据字段
 * json数据格式：{"eventType":"buy","userId":"A10001","platform":"web","ts":1590396112667}
 */

public class FlinkPVData implements Serializable {
    private String eventType;
    private String userId;
    private String platform;
    private long ts;

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getPlatform() {
        return platform;
    }

    public void setPlatform(String platform) {
        this.platform = platform;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    @Override
    public String toString() {
        return "{\"eventType\":\"" + this.eventType + "\"}";
    }
}
