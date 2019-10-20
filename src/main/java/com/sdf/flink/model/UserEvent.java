package com.sdf.flink.model;

import com.alibaba.fastjson.JSON;

import java.io.Serializable;

/**
 * 定义用户购物日志数据字段
 * 数据格式：{"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"PURCHASE",
 * "eventTime":"2018-06-12 09:30:28","data":{"productId":196,"price":600.00,"amount":600.00}}
 */
public class UserEvent implements Serializable {
    private String userId;
    private String channel;
    private String eventType;
    private String eventTime;
    private String data;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getEventTime() {
        return eventTime;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public UserEvent() {
    }

    public UserEvent(String userId, String channel, String eventType, String eventTime, String data) {
        this.userId = userId;
        this.channel = channel;
        this.eventType = eventType;
        this.eventTime = eventTime;
        this.data = data;
    }

    /**
     * @param value 用户浏览商品事件数据
     * @return UserEvent
     */
    public static UserEvent buildEvent(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        } else {
            return JSON.parseObject(value, UserEvent.class);
        }
    }

    @Override
    public String toString() {
        return "{userId:" + this.userId +
                ", channel:" + this.channel +
                ", eventType:" + this.eventType +
                ", eventTime:" + this.eventTime +
                ", data:" + this.data + "}";
    }
}
