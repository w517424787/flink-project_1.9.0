package com.sdf.flink.streaming;

import com.sdf.flink.sink.SinkToHBase;
import com.sdf.flink.util.KafkaConfigUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class GetDataFromKafkaSinkToHBase {
    public static void main(String[] args) throws Exception {
        final String broker_list = "192.168.7.113:9092,192.168.7.114:9092,192.168.7.115:9092";
        final String group_id = "group_usernetworkdelay";
        String flag = "dev";
        if (args.length > 1) {
            flag = args[0];
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(2);
        env.enableCheckpointing(3600000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //kafka属性
        Properties properties = KafkaConfigUtil.buildConsumerProps(broker_list, group_id);

        @SuppressWarnings("unchecked")
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer("usernetworkdelay",
                new SimpleStringSchema(), properties);
        //设置offset读取位置
        consumer.setStartFromGroupOffsets(); //默认从group.id中读取

        DataStreamSource<String> dataStreamSource = env.addSource(consumer);
        dataStreamSource.addSink(new SinkToHBase(flag)).setParallelism(1);

        env.execute("Sink_To_HBase");
    }
}
