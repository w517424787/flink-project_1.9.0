package com.sdf.flink.model;

import java.util.ArrayList;
import java.util.List;

public class UserEventContainer {
    private String userId;
    private List<UserEvent> userEventList = new ArrayList<>();

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public List<UserEvent> getUserEventList() {
        return userEventList;
    }

    public void setUserEventList(List<UserEvent> userEventList) {
        this.userEventList = userEventList;
    }

}
