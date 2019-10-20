package com.sdf.flink;

import com.sdf.flink.util.ConvertDateUtils;

public class test {
    public static void main(String[] args) {
        System.out.println(ConvertDateUtils.convertDateToLong("2019-10-20 09:20:10","yyyy-MM-dd HH:mm:ss"));
    }
}
