package com.sdf.flink.model;

public class CityConfig {
    private Integer cityId;
    private String cityDesc;

    public Integer getCityId() {
        return cityId;
    }

    public void setCityId(Integer cityId) {
        this.cityId = cityId;
    }

    public String getCityDesc() {
        return cityDesc;
    }

    public void setCityDesc(String cityDesc) {
        this.cityDesc = cityDesc;
    }

    public CityConfig() {

    }

    public CityConfig(Integer cityId, String cityDesc) {
        this.cityDesc = cityDesc;
        this.cityId = cityId;
    }
}
