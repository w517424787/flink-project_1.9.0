package com.sdf.flink.model;

import com.alibaba.fastjson.JSON;

/**
 * 定义用户配置字段
 * JSON数据格式：
 * {"channel":"APP","registerDate":"2018-01-01","historyPurchaseTimes":0,"maxPurchasePathLength":3}
 */
public class Config {
    private String channel;
    private String registerDate;
    private int historyPurchaseTimes;
    private int maxPurchasePathLength;

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getRegisterDate() {
        return registerDate;
    }

    public void setRegisterDate(String registerDate) {
        this.registerDate = registerDate;
    }

    public int getHistoryPurchaseTimes() {
        return historyPurchaseTimes;
    }

    public void setHistoryPurchaseTimes(int historyPurchaseTimes) {
        this.historyPurchaseTimes = historyPurchaseTimes;
    }

    public int getMaxPurchasePathLength() {
        return maxPurchasePathLength;
    }

    public void setMaxPurchasePathLength(int maxPurchasePathLength) {
        this.maxPurchasePathLength = maxPurchasePathLength;
    }

    public Config() {

    }

    public Config(String channel, String registerDate, int historyPurchaseTimes, int maxPurchasePathLength) {
        this.channel = channel;
        this.registerDate = registerDate;
        this.historyPurchaseTimes = historyPurchaseTimes;
        this.maxPurchasePathLength = maxPurchasePathLength;
    }

    public static Config buildConfig(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        } else {
            return JSON.parseObject(value, Config.class);
        }
    }

    @Override
    public String toString() {
        return "{channel:" + this.channel +
                ", registerDate:" + this.registerDate +
                ", historyPurchaseTimes:" + this.historyPurchaseTimes +
                ", maxPurchasePathLength:" + this.maxPurchasePathLength + "}";
    }
}
