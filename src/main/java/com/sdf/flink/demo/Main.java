package com.sdf.flink.demo;

import java.util.ResourceBundle;

public class Main {

    private static String DriverName = "";
    private static String URL = "";
    private static String Password = "";

    public static void main(String[] args) {
        DriverName = ResourceBundle.getBundle("dbConfig").getString("driverName");
        URL = ResourceBundle.getBundle("dbConfig").getString("url");
        Password = ResourceBundle.getBundle("dbConfig").getString("password");
        System.out.println(DriverName);
        System.out.println(URL);
        System.out.println(Password);
    }
}
