package com.sdf.flink.function;

import com.alibaba.fastjson.JSONObject;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.sdf.flink.model.CityConfig;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.*;

/**
 * Modify by wwg 2020-06-28
 * 维度关联
 */

public class WholeLoad extends RichMapFunction<String, CityConfig> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WholeLoad.class);
    private transient ScheduledExecutorService executorService = null;
    private transient volatile Cache<Integer, String> cityDescCache;
    private static Connection connection = null;

    @Override
    public CityConfig map(String value) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(value);
        Integer cityId = jsonObject.getInteger("cityId");
        //从缓存中加载数据
        String cityDesc = cityDescCache.getIfPresent(cityId);
        return new CityConfig(cityId, cityDesc);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //缓存设置，最大条数20000，过期时间1天
        cityDescCache = CacheBuilder.newBuilder().maximumSize(20000).expireAfterWrite(1, TimeUnit.DAYS).build();
        //初始化多线程
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(3, 3, 0L,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        executorService = Executors.newScheduledThreadPool(3, threadPoolExecutor.getThreadFactory());

//        executorService = new ScheduledThreadPoolExecutor(1, new BasicThreadFactory.Builder()
//                .namingPattern("example-schedule-pool-%d").daemon(true).build());
        //executorService = new ScheduledThreadPoolExecutor(3);
//        executorService = new ScheduledThreadPoolExecutor(1, r -> {
//            Thread thread = new Thread(r, "update-cityId");
//            thread.setUncaughtExceptionHandler((t, e) -> {
//                LOGGER.error("Thread " + t + " got uncaught exception: " + e.getMessage());
//            });
//            return thread;
//        });

        if (connection == null) {
            getConnection();
        }
        //加载维度数据
        //每隔10分钟全量加载一次维度
        //scheduleAtFixedRate 和 scheduleWithFixedDelay区别：
        //scheduleAtFixedRate是按固定的period进行周期性调度，不考虑调度延迟情况
        //scheduleWithFixedDelay是根据上一次执行完+delay进行下一次调度，更加灵活一点，不会出现调度间隔太短情况
        //executorService.scheduleAtFixedRate(new Runnable() {
        executorService.scheduleWithFixedDelay(() -> {
            try {
                //加载数据
                load();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }, 0, 10 * 60, TimeUnit.SECONDS);
    }

    public void load() throws Exception {
        PreparedStatement statement = connection.prepareStatement("select cityId,cityDesc from dim_city");
        ResultSet resultSet = statement.executeQuery();
        while (resultSet.next()) {
            Integer cityId = resultSet.getInt("cityId");
            String cityDesc = resultSet.getString("cityDesc");
            cityDescCache.put(cityId, cityDesc);
        }
    }

    /**
     * 创建mysql连接串
     */
    private static void getConnection() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String url = "jdbc:mysql://192.168.7.105:3306/flink_test?useUnicode=true&characterEncoding=UTF-8";
            String user = "root";
            String pwd = "steve201718";
            connection = DriverManager.getConnection(url, user, pwd);
        } catch (Exception e) {
            System.out.println("mysql get connection has exception , msg = " + e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
    }
}
