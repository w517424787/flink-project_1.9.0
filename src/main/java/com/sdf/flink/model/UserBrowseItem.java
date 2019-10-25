package com.sdf.flink.model;

public class UserBrowseItem {
    private String userId;
    private String itemId;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public UserBrowseItem() {

    }

    public UserBrowseItem(String userId, String itemId) {
        this.userId = userId;
        this.itemId = itemId;
    }
}
