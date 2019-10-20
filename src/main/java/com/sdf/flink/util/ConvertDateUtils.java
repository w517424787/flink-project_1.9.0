package com.sdf.flink.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class ConvertDateUtils {

    /**
     * 将string类型的时间转换成long数值
     *
     * @param date
     * @param format
     * @return
     */
    public static long convertDateToLong(String date, String format) {
        if (date == null || date.isEmpty()) {
            return 0L;
        } else {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
            LocalDateTime parse = LocalDateTime.parse(date, formatter);
            return LocalDateTime.from(parse).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        }
    }
}
