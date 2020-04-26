package com.sdf.flink.source;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 模拟点击数据，将数据put到kafka中
 */

public class MyKafkaSource extends RichSourceFunction<String> {

    private boolean isRunning = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isRunning) {
            List<String> books = new ArrayList<>();
            books.add("Python");
            books.add("Java");
            books.add("PHP");
            books.add("C++");
            books.add("Scala");
            books.add("Flink");
            books.add("Spark");
            books.add("kafka");

            int i = new Random().nextInt(8);
            ctx.collect(books.get(i));

            //模拟每1秒中产生一条数据
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
