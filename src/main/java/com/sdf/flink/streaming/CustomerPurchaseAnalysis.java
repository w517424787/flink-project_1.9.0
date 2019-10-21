package com.sdf.flink.streaming;

import com.sdf.flink.function.ConnectedBroadcastProcessFunction;
import com.sdf.flink.model.Config;
import com.sdf.flink.model.UserEvent;
import com.sdf.flink.util.ConvertDateUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * 用户购物行为路径分析，分析用户购物时路径长度
 * 用户在APP上购物行为：VIEW_PRODUCT、ADD_TO_CART、REMOVE_FROM_CART、PURCHASE
 * 用户在APP上购物行为数据格式为JSON，如：
 * {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"VIEW_PRODUCT","eventTime":"2018-06-12 09:27:25","data":{"productId":196}}
 * {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"ADD_TO_CART","eventTime":"2018-06-12 09:27:35","data":{"productId":196}}
 * {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"VIEW_PRODUCT","eventTime":"2018-06-12 09:27:11","data":{"productId":126}}
 * {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"PURCHASE",
 * "eventTime":"2018-06-12 09:30:28","data":{"productId":196,"price":600.00,"amount":600.00}}
 * 可以根据动态配置购物路径长度，进行灵活判断，格式也是为JSON：
 * {"channel":"APP","registerDate":"2018-01-01","historyPurchaseTimes":0,"maxPurchasePathLength":3}
 * 只输出用户购物路径超过定义的长度的用户，并将数据返回到Kafka Topic中，格式：
 * {"userId":"a9b83681ba4df17a30abcf085ce80a9b","channel":"APP","purchasePathLength":9,"eventTypeCounts":{"ADD_TO_CART":1,"PURCHASE":1,"VIEW_PRODUCT":7}}
 * 定义三个Kafka Topic:input-event-topic,input-config-topic,output-topic
 */

public class CustomerPurchaseAnalysis {

    private static final Logger LOG = LoggerFactory.getLogger(CustomerPurchaseAnalysis.class);

    //定义广播状态
    private static final MapStateDescriptor<String, Config> mapStateDescriptor =
            new MapStateDescriptor<>("configBroadcastState",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    TypeInformation.of(new TypeHint<Config>() {
                    }));

    //定义水印
    private static class CustomWatermarkExtractor extends BoundedOutOfOrdernessTimestampExtractor<UserEvent> {

        private CustomWatermarkExtractor(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(UserEvent element) {
            return ConvertDateUtils.convertDateToLong(element.getEventTime(), "yyyy-MM-dd HH:mm:ss");
        }
    }

    public static void main(String[] args) throws Exception {
        LOG.info("Input args:" + Arrays.asList(args));

        //判断输入参数格式
        final ParameterTool parameters = ParameterTool.fromArgs(args);
        if (parameters.getNumberOfParameters() < 5) {
            System.out.println("Missing parameters!\n" +
                    "Usage: Kafka --input-event-topic <topic> --input-config-topic <topic> --output-topic <topic> " +
                    "--bootstrap.servers <kafka brokers> " +
                    "--zookeeper.connect <zk quorum> --group.id <group id>");
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置状态checkpoint路径
        env.setStateBackend(new FsStateBackend("hdfs://192.168.7.111:8020/flink/checkpoint/customer-purchase"));

        //设置checkpoint参数
        CheckpointConfig checkpointConfig = new CheckpointConfig();
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setCheckpointInterval(60 * 60 * 1000); //1小时触发一次
        checkpointConfig.setCheckpointTimeout(10 * 60 * 1000); //设置Timeout时间

        env.getConfig().setGlobalJobParameters(parameters);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // create customer user event stream
        final FlinkKafkaConsumer011 kafkaUserEventSource = new FlinkKafkaConsumer011<>(
                parameters.getRequired("input-event-topic"),
                new SimpleStringSchema(), parameters.getProperties());

        //将流数据转换成(UserEvent,userId)格式
        final KeyedStream<UserEvent, String> customerUserEventStream =
                env.addSource(kafkaUserEventSource).map(value -> UserEvent.buildEvent(value.toString()))
                        .assignTimestampsAndWatermarks(new CustomWatermarkExtractor(Time.minutes(10)))
                        .keyBy(new KeySelector<UserEvent, String>() {
                            @Override
                            public String getKey(UserEvent event) throws Exception {
                                return event.getUserId();
                            }
                        });

        //读取配置参数数据流
        final FlinkKafkaConsumer011 kafkaConfigEventSource = new FlinkKafkaConsumer011<>(
                parameters.getRequired("input-config-topic"),
                new SimpleStringSchema(), parameters.getProperties());

        //进行广播
        final BroadcastStream<Config> configBroadcastStream =
                env.addSource(kafkaConfigEventSource).map(value -> Config.buildConfig(value.toString()))
                        .broadcast(mapStateDescriptor);

        //将结果输出到Kafka Topic中
        final FlinkKafkaProducer011 kafkaProducer011 =
                new FlinkKafkaProducer011(parameters.getRequired("output-topic"),
                        new SimpleStringSchema(), parameters.getProperties());

        //连接两个流数据
        customerUserEventStream.connect(configBroadcastStream)
                .process(new ConnectedBroadcastProcessFunction());

    }
}
