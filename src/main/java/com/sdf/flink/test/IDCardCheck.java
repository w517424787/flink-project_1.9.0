package com.sdf.flink.test;

import com.sdf.flink.util.isIdNum;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;

public class IDCardCheck {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        DataSet<String> text = env.readTextFile("E:\\flink-demo\\flink-project_1.9.0\\data\\idcard2.txt");
        text.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                String[] array = value.split(",");
                return isIdNum.isValidatedAllIdcard(array[0]);
            }
        }).print();

        //env.execute("Flink Word Count Demo");
    }
}
