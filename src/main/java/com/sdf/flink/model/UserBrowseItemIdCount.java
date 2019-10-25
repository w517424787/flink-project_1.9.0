package com.sdf.flink.model;

public class UserBrowseItemIdCount {
    private String itemId;
    private Integer orderCount;
    private Long timeWindow;

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public Integer getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(Integer orderCount) {
        this.orderCount = orderCount;
    }

    public Long getTimeWindow() {
        return timeWindow;
    }

    public void setTimeWindow(Long timeWindow) {
        this.timeWindow = timeWindow;
    }

    public UserBrowseItemIdCount() {

    }

    public UserBrowseItemIdCount(String itemId, Integer orderCount, Long timeWindow) {
        this.itemId = itemId;
        this.orderCount = orderCount;
        this.timeWindow = timeWindow;
    }
}
