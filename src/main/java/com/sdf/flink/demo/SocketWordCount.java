package com.sdf.flink.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink读取本地端口进行数据统计
 * nc -l 9999
 */
public class SocketWordCount {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("USAGE: SocketTextStreamWordCount <hostname> <port>");
            System.exit(1);
        }

        String hostName = args[0];
        int port = Integer.valueOf(args[1]);

        //创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream(hostName, port).flatMap(new LineSplitter()).keyBy(0).sum(1).print();

        env.execute("SocketWordCount");
    }

    /**
     * 字符分割
     */
    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.toLowerCase().split("\\W+");
            for (String word : words) {
                if (word.length() > 0) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
}


