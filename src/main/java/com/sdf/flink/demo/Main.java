package com.sdf.flink.demo;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.ResourceBundle;

public class Main {

    private static String DriverName = "";
    private static String URL = "";
    private static String Password = "";

    public static void main(String[] args) throws Exception {
        DriverName = ResourceBundle.getBundle("dbConfig").getString("driverName");
        URL = ResourceBundle.getBundle("dbConfig").getString("url");
        Password = ResourceBundle.getBundle("dbConfig").getString("password");
        System.out.println(DriverName);
        System.out.println(URL);
        System.out.println(Password);

        String str = "2020-04-26 16:33:45:433";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = sdf.parse(str);
        System.out.println(new SimpleDateFormat("yyyyMMddHHmmss").format(date));

        System.out.println(Long.valueOf(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                .parse("2020-04-30 16:04:54").getTime() / 1000));
    }
}
