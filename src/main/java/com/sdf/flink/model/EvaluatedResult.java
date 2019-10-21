package com.sdf.flink.model;

/**
 * 定义输出结果
 * 数据格式：{"userId":"a9b83681ba4df17a30abcf085ce80a9b","channel":"APP","purchasePathLength":9,
 * "eventTypeCounts":{"ADD_TO_CART":1,"PURCHASE":1,"VIEW_PRODUCT":7}}
 */
public class EvaluatedResult {
    private String userId;
    private String channel;
    private int purchasePathLength;
    private String eventTypeCounts;

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

    public int getPurchasePathLength() {
        return purchasePathLength;
    }

    public void setPurchasePathLength(int purchasePathLength) {
        this.purchasePathLength = purchasePathLength;
    }

    public String getEventTypeCounts() {
        return eventTypeCounts;
    }

    public void setEventTypeCounts(String eventTypeCounts) {
        this.eventTypeCounts = eventTypeCounts;
    }

    public EvaluatedResult(String userId, String channel, int purchasePathLength, String eventTypeCounts) {
        this.userId = userId;
        this.channel = channel;
        this.purchasePathLength = purchasePathLength;
        this.eventTypeCounts = eventTypeCounts;
    }

    @Override
    public String toString() {
        return "{userId:" + this.userId +
                ", channel:" + this.channel +
                ", purchasePathLength:" + this.purchasePathLength +
                ", eventTypeCounts:{" + this.eventTypeCounts + "}}";
    }
}
