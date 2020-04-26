package com.sdf.flink.demo;

import java.util.*;

public class TreeMapDemo {
    public static void main(String[] args) {
        TreeMap<Integer, String> treeMap = new TreeMap<>(new Comparator<Integer>() {
            @Override
            public int compare(Integer x, Integer y) {
                //排序
                return x < y ? 1 : x.equals(y) ? 0 : -1;
            }
        });

        treeMap.put(1, "Flink");
        treeMap.put(3, "Spark");
        treeMap.put(2, "Kafka");
        treeMap.put(4, "Kafka");

        //移除treeMap最开始的那条数据
        System.out.println(treeMap.pollFirstEntry());
        System.out.println(treeMap.toString());
        System.out.println(treeMap.keySet().toString());
        System.out.println(treeMap.values().toString());
        System.out.println(treeMap.get(1));

        Set<Map.Entry<Integer, String>> set = treeMap.entrySet();
        System.out.println(set);

        Iterator<Map.Entry<Integer, String>> iter = set.iterator();
        while (iter.hasNext()) {
            Map.Entry<Integer, String> map = iter.next();
            System.out.println(map.getKey());
            System.out.println(map.getValue());
        }
    }
}
