package com.sdf.flink.streaming;

import com.alibaba.druid.pool.DruidDataSource;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.ExecutorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

/**
 * Modify by wwg 2020-06-03
 * 通过异步的方式(RichAsyncFunction)进行维表关联操作
 * 通过市ID去转换市的名称
 * 如：cityID：5110，cityDesc：内江市
 */

public class FlinkStreamingDimAsyncJoin {

    public static final class AsyncDimJoin extends RichAsyncFunction<Integer, Tuple2<Integer, String>> {
        private static final Logger LOG = LoggerFactory.getLogger(AsyncDimJoin.class);
        private transient DruidDataSource druidDataSource;
        private transient ListeningExecutorService executorService;
        private transient volatile Cache<Integer, String> cityDescCache;

        @Override
        public void asyncInvoke(Integer input, ResultFuture<Tuple2<Integer, String>> resultFuture) {
            //异常多线程处理
            executorService.submit(() -> {
                CompletableFuture.supplyAsync(() -> {
                    try {
                        //根据输入cityId去查找cityDesc
                        return getCityDescFromCache(input);
                    } catch (Exception ex) {
                        LOG.error(ex.getMessage());
                        return null;
                    }
                }).thenAccept((String result) ->
                        resultFuture.complete(Collections.singleton(Tuple2.of(input, result))));
            });
        }

        /**
         * 从Cache中获取cityDesc，如果缓存不存在，直接去读取数据库
         *
         * @param cityId
         * @return
         */
        public String getCityDescFromCache(Integer cityId) {
            return cityDescCache.getIfPresent(cityId) != null ? cityDescCache.getIfPresent(cityId) :
                    getCityDescFromMysql(cityId);
        }

        /**
         * 从mysql中读取数据，存入到guava缓存
         *
         * @param cityId
         * @return
         */
        public String getCityDescFromMysql(Integer cityId) {
            final String sql = "SELECT cityId,cityDesc FROM dim_city WHERE cityId = " + cityId + ";";
            PreparedStatement ps;
            String cityDesc = "";
            //获取连接
            try (Connection connection = druidDataSource.getConnection()) {
                ps = connection.prepareStatement(sql);
                ResultSet resultSet = ps.executeQuery();
                while (resultSet.next()) {
                    cityDesc = resultSet.getString("cityDesc");
                    //添加到缓存中
                    cityDescCache.put(cityId, cityDesc);
                }
            } catch (Exception ex) {
                LOG.info(ex.getMessage());
            }
            return cityDesc;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //初始化多线程
            executorService = MoreExecutors.listeningDecorator(new ThreadPoolExecutor(10, 10, 60000L,
                    TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>(500)));
            //缓存设置
            cityDescCache = CacheBuilder.newBuilder().maximumSize(2000).expireAfterWrite(1, TimeUnit.DAYS).build();

            //建立druid线程池
            druidDataSource = new DruidDataSource();
            druidDataSource.setDriverClassName("com.mysql.jdbc.Driver");
            druidDataSource.setUsername("root");
            druidDataSource.setPassword("steve201718");
            druidDataSource.setUrl("jdbc:mysql://192.168.7.105:3306/flink_test?useUnicode=true&characterEncoding=UTF-8");
            druidDataSource.setInitialSize(20);
            druidDataSource.setMinIdle(20);
            druidDataSource.setMaxActive(40);
        }

        @Override
        public void close() throws Exception {
            super.close();
            druidDataSource.close();
            //关闭连接
            ExecutorUtils.gracefulShutdown(60, TimeUnit.SECONDS, executorService);
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取数据
        Integer[] cityIds = {5101, 5103, 5104, 5105, 5106, 5107, 5108};
        List<Integer> cityList = Arrays.asList(cityIds);
        DataStream<Integer> dataStream = env.fromCollection(cityList);

        SingleOutputStreamOperator<Tuple2<Integer, String>> outputStream = AsyncDataStream.unorderedWait(dataStream,
                new AsyncDimJoin(), 60, TimeUnit.SECONDS, 10);

        outputStream.print();
        env.execute("Flink Dim Join");
    }
}
