package com.sdf.flink.streaming;

import com.sdf.flink.model.*;
import com.sdf.flink.util.ConvertDateUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

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
 * 定义三个Kafka Topic:input-event-topic,input-config.properties-topic,output-topic
 */

public class CustomerPurchaseAnalysis {

    private static final Logger LOG = LoggerFactory.getLogger(CustomerPurchaseAnalysis.class);
    private static final Config DEFAULT_CONFIG = new Config("APP", "2019-10-20", 0, 20);

    //定义广播状态
    private static final MapStateDescriptor<String, Config> configStateDescriptor =
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
            return element.getEventTimestamp();
        }
    }

    //定义广播函数
    private static class ConnectedBroadcastProcessFunction extends KeyedBroadcastProcessFunction<Object,
            UserEvent, Config, EvaluatedResult> {

        private static final long serialVersionUID = 1L;
        //定义state格式：(channel, Map<userId, UserEventContainer>)
        private final MapStateDescriptor<String, Map<String, UserEventContainer>> userMapStateDesc =
                new MapStateDescriptor<>("userEventContainerState", BasicTypeInfo.STRING_TYPE_INFO,
                        new MapTypeInfo<>(String.class, UserEventContainer.class));

        @Override
        public void processElement(UserEvent value, ReadOnlyContext ctx, Collector<EvaluatedResult> out) throws Exception {
            String userId = value.getUserId();
            String channel = value.getChannel();
            EventType eventType = EventType.valueOf(value.getEventType());
            Config config = ctx.getBroadcastState(configStateDescriptor).get(channel);
            LOG.info("Read config.properties: channel=" + channel + ", config.properties=" + config);
            //判断是否为空
            if (Objects.isNull(config)) {
                config = DEFAULT_CONFIG;
            }

            final MapState<String, Map<String, UserEventContainer>> state =
                    getRuntimeContext().getMapState(userMapStateDesc);
            // collect per-user events to the user map state
            Map<String, UserEventContainer> userEventContainerMap = state.get(channel);
            //判断是否为空
            if (Objects.isNull(userEventContainerMap)) {
                userEventContainerMap = Maps.newHashMap();
                state.put(channel, userEventContainerMap);
            }
            //如果没有包含该userId信息，则添加
            if (!userEventContainerMap.containsKey(userId)) {
                UserEventContainer container = new UserEventContainer();
                container.setUserId(userId);
                userEventContainerMap.put(userId, container);
            }
            //如果存在，则添加值
            userEventContainerMap.get(userId).getUserEventList().add(value);

            // check whether a user purchase event arrives
            // if true, then compute the purchase path length, and prepare to trigger predefined actions
            if (eventType == EventType.PURCHASE) {
                LOG.info("Receive a purchase event: " + value);

                //计算玩家购物路径
                Optional<EvaluatedResult> result = compute(config, userEventContainerMap.get(userId));
                result.ifPresent(r -> out.collect(result.get()));
                // clear evaluated user's events
                state.get(channel).remove(userId);
            }
        }

        /**
         * 计算玩家购物路径
         *
         * @param config
         * @param container
         * @return
         */
        private Optional<EvaluatedResult> compute(Config config, UserEventContainer container) {
            Optional<EvaluatedResult> result = Optional.empty();

            //根据渠道进行配置
            String channel = config.getChannel();
            int historyPurchaseTimes = config.getHistoryPurchaseTimes();
            int maxPurchasePathLength = config.getMaxPurchasePathLength();
            int purchasePathLen = container.getUserEventList().size();

            //当购物完成时路径大于配置的最长路径就进行计算
            if (historyPurchaseTimes < 10 && purchasePathLen > maxPurchasePathLength) {
                // sort by event time
                container.getUserEventList().sort(Comparator.comparingLong(UserEvent::getEventTimestamp));
                final Map<String, Integer> stat = Maps.newHashMap();
                container.getUserEventList()
                        .stream()
                        .collect(Collectors.groupingBy(UserEvent::getEventType))
                        .forEach((eventType, events) -> stat.put(eventType, events.size()));

                //输出购物记录
                final EvaluatedResult evaluatedResult = new EvaluatedResult();
                evaluatedResult.setUserId(container.getUserId());
                evaluatedResult.setChannel(channel);
                evaluatedResult.setEventTypeCounts(stat);
                evaluatedResult.setPurchasePathLength(purchasePathLen);

                LOG.info("Evaluated result: " + evaluatedResult.toString());
                result = Optional.of(evaluatedResult);
            }
            return result;
        }

        @Override
        public void processBroadcastElement(Config value, Context ctx, Collector<EvaluatedResult> out) throws Exception {
            String channel = value.getChannel();
            BroadcastState<String, Config> state = ctx.getBroadcastState(configStateDescriptor);
            final Config oldConfig = ctx.getBroadcastState(configStateDescriptor).get(channel);
            if (state.contains(channel)) {
                LOG.info("Configured channel exists: channel=" + channel);
                LOG.info("Config detail: oldConfig=" + oldConfig + ", newConfig=" + value);
            } else {
                LOG.info("Config detail: defaultConfig=" + DEFAULT_CONFIG + ", newConfig=" + value);
            }

            // update config.properties value for config.properties key
            state.put(channel, value);
        }
    }

    public static void main(String[] args) throws Exception {
        LOG.info("Input args:" + Arrays.asList(args));

        //判断输入参数格式
        final ParameterTool parameters = ParameterTool.fromArgs(args);
        if (parameters.getNumberOfParameters() < 5) {
            System.out.println("Missing parameters!\n" +
                    "Usage: Kafka --input-event-topic <topic> --input-config.properties-topic <topic> --output-topic <topic> " +
                    "--bootstrap.servers <kafka brokers> " +
                    "--zookeeper.connect <zk quorum> " +
                    "--group.id <group id>");
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置水印时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置水印产生周期(5s)，默认是200ms
        env.getConfig().setAutoWatermarkInterval(5000L);
        env.getConfig().setGlobalJobParameters(parameters);
        //env.setStateBackend(new RocksDBStateBackend("hdfs://192.168.7.111:8020/flink/checkpoint/customer-purchase", true));

        //设置状态checkpoint路径
        env.setStateBackend(new FsStateBackend("hdfs://192.168.7.111:8020/flink/checkpoint/customer-purchase"));

        //设置checkpoint参数
        CheckpointConfig checkpointConfig = new CheckpointConfig();
        // 任务流取消和故障时会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置模式为exactly-once
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置checkpoint触发时间
        checkpointConfig.setCheckpointInterval(60 * 60 * 1000); //1小时触发一次
        // 设置checkpoint超时时间
        checkpointConfig.setCheckpointTimeout(10 * 60 * 1000); //设置Timeout时间
        // 设置checkpoint同时最大允许进行几个检查点
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // 确保检查点之间有至少60 * 1000的间隔【checkpoint最小间隔】
        checkpointConfig.setMinPauseBetweenCheckpoints(60 * 1000);

        // create customer user event stream
        final FlinkKafkaConsumer kafkaUserEventSource = new FlinkKafkaConsumer<>(
                parameters.getRequired("input-event-topic"),
                new SimpleStringSchema(), parameters.getProperties());

        //将流数据转换成(UserEvent,userId)格式
        @SuppressWarnings("unchecked") final KeyedStream<UserEvent, String> customerUserEventStream =
                env.addSource(kafkaUserEventSource).map(value -> UserEvent.buildEvent(value.toString()))
                        .assignTimestampsAndWatermarks(new CustomWatermarkExtractor(Time.minutes(10)))
                        .keyBy(new KeySelector<UserEvent, String>() {
                            @Override
                            public String getKey(UserEvent event) throws Exception {
                                return event.getUserId();
                            }
                        });

        //读取配置参数数据流
        final FlinkKafkaConsumer kafkaConfigEventSource = new FlinkKafkaConsumer<>(
                parameters.getRequired("input-config.properties-topic"),
                new SimpleStringSchema(), parameters.getProperties());

        //进行广播
        @SuppressWarnings("unchecked") final BroadcastStream<Config> configBroadcastStream =
                env.addSource(kafkaConfigEventSource).map(value -> Config.buildConfig(value.toString()))
                        .broadcast(configStateDescriptor);

        //将结果输出到Kafka Topic中
        @SuppressWarnings("unchecked") final FlinkKafkaProducer kafkaProducer011 =
                new FlinkKafkaProducer(parameters.getRequired("output-topic"),
                        new SimpleStringSchema(), parameters.getProperties());

        //连接两个流数据
        DataStream<EvaluatedResult> connectedStream = customerUserEventStream.connect(configBroadcastStream)
                .process(new ConnectedBroadcastProcessFunction());

        //输出到Kafka中
        connectedStream.addSink(kafkaProducer011);

        env.execute("CustomerPurchaseAnalysis");
    }
}
