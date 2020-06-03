package com.sdf.flink.streaming;

import com.alibaba.druid.util.JdbcUtils;
import com.sdf.flink.util.JdbcUtil;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExecutorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Modify by wwg 2020-06-02
 * 通过读取mysql中的维表数据进行join，这里通过线程池的方式去加载维度信息
 * 通过市ID去转换市的名称
 * 如：cityID：5110，cityDesc：内江市
 */

public class FlinkStreamingDimJoin2 {

    /**
     * 通过线程方式去加载维度信息，适用于轻量级的维度数据
     */
    public static final class DimThreadFlatFunction extends RichFlatMapFunction<Integer, Tuple2<Integer, String>> {
        private static final long serialVersionUID = 1L;
        private static final Logger LOG = LoggerFactory.getLogger(DimThreadFlatFunction.class);
        //cityId -> cityDesc
        private Map<Integer, String> dim;
        private transient ScheduledExecutorService dbScheduler;
        private static final String sql = "SELECT cityId,cityDesc FROM dim_city;";

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            dim = new HashMap<>(1024);
            dbScheduler = new ScheduledThreadPoolExecutor(1, r -> {
                Thread thread = new Thread(r, "update-cityId");
                thread.setUncaughtExceptionHandler((t, e) -> {
                    LOG.error("Thread " + t + " got uncaught exception: " + e.getMessage());
                });
                return thread;
            });

            //执行SQL查询数据
            dbScheduler.scheduleWithFixedDelay(() -> {
                try {
                    QueryRunner queryRunner = new QueryRunner(JdbcUtil.getDataSource());
                    List<Map<String, Object>> cityInfo = queryRunner.query(sql, new MapListHandler());
                    for (Map<String, Object> item : cityInfo) {
                        Integer cityId = Integer.parseInt(item.get("cityId").toString());
                        String cityDesc = item.get("cityDesc").toString();
                        //添加数据
                        dim.put(cityId, cityDesc);
                    }
                } catch (Exception ex) {
                    LOG.error("Exception occurred when querying: " + ex.getMessage());
                }
            }, 0, 10 * 60, TimeUnit.SECONDS);
        }

        @Override
        public void close() throws Exception {
            super.close();
            //dim.clear();
            //关闭连接
            ExecutorUtils.gracefulShutdown(60, TimeUnit.SECONDS, dbScheduler);
        }

        @Override
        public void flatMap(Integer value, Collector<Tuple2<Integer, String>> out) throws Exception {
            out.collect(Tuple2.of(value, dim.get(value)));
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取数据
        Integer[] cityIds = {5101, 5103, 5104, 5105, 5106, 5107, 5108};
        List<Integer> cityList = Arrays.asList(cityIds);
        DataStream<Integer> dataStream = env.fromCollection(cityList);

        SingleOutputStreamOperator<Tuple2<Integer, String>> outputStream =
                dataStream.flatMap(new DimThreadFlatFunction());

        outputStream.print();
        env.execute("Flink Dim Join");
    }
}
