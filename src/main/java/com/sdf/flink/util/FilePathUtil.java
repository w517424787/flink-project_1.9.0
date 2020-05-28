package com.sdf.flink.util;

import java.io.File;

public class FilePathUtil {

    /**
     * 根据输入的文件名获取文件的路径
     *
     * @param fileName
     * @return
     */
    public static String getFilePath(String fileName) throws Exception {
        return new File("data\\" + fileName).getCanonicalPath();
    }
}
