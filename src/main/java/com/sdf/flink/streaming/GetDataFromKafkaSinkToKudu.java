package com.sdf.flink.streaming;

import com.sdf.flink.sink.SinkToKudu;
import com.sdf.flink.util.KafkaUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 从kafka中某个topic读取数据，将数据插入到Kudu中
 */

public class GetDataFromKafkaSinkToKudu {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(2);
        env.enableCheckpointing(300000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //kafka属性
        Properties properties = KafkaUtils.getKafkaProperties();
        
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer("usernetworkdelay",
                new SimpleStringSchema(), properties);
        //设置offset读取位置
        consumer.setStartFromGroupOffsets(); //默认从group.id中读取
        //consumer.setStartFromEarliest(); //从最早的记录开始
        //consumer.setStartFromLatest(); //从最新记录开始
        //consumer.setStartFromTimestamp(100000); //从指定的epoch时间戳（毫秒）开始;

        DataStreamSource<String> dataStreamSource = env.addSource(consumer).setParallelism(1);

        dataStreamSource.addSink(new SinkToKudu("192.168.7.111:7051", 4000));

        env.execute("Sink To Kudu");
    }
}
