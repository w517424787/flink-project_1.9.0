package com.sdf.flink.sql

import java.util

import com.sdf.flink.sink.SinkPVToKudu
import com.sdf.flink.util.Logger
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
  * Modify by wwg 2020-05-25
  * 通过flink sql进行pv、uv统计
  */

object FlinkSQLPVDemo extends Logger {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //每60s触发一次
    streamEnv.enableCheckpointing(60 * 1000)
    streamEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //streamEnv.setStateBackend(new FsStateBackend("hdfs://192.168.7.111:8020/flink/checkpoint"))

    //checkpoint保留方式，手动取消时删除
    streamEnv.getCheckpointConfig.enableExternalizedCheckpoints(
      CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 设置checkpoint超时时间
    streamEnv.getCheckpointConfig.setCheckpointTimeout(30 * 1000) //设置Timeout时间
    // 设置checkpoint同时最大允许进行几个检查点
    streamEnv.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 确保检查点之间有至少30 * 1000的间隔【checkpoint最小间隔】
    streamEnv.getCheckpointConfig.setMinPauseBetweenCheckpoints(30 * 1000)

    val tableEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(streamEnv, tableEnvSettings)

    //创建hive catalog
    //指定hive-site.xml存储位置，创建hive的catalog，指定默认的database
    val catalog = new HiveCatalog("rtdw", "default", "/data01/bigdata/flink-1.10.0/conf", "1.1.0")
    tableEnv.registerCatalog("rtdw", catalog)
    //指定到对应的catalog
    tableEnv.useCatalog("rtdw")

    //创建database
    val createDBSql = "CREATE DATABASE IF NOT EXISTS rtdw.flink_ods"
    tableEnv.sqlUpdate(createDBSql)

    //create table不支持if not exists，先判断是否存在表，存在就删除
    val dropTableSql = "DROP TABLE IF EXISTS rtdw.flink_ods.streaming_user_active_log"
    tableEnv.sqlUpdate(dropTableSql)

    //创建hive table
    //create table不支持if not exists
    val createTableSql =
    """
      | CREATE TABLE rtdw.flink_ods.streaming_user_active_log (
      |  eventType STRING COMMENT 'eventType',
      |  userId STRING,
      |  platform STRING,
      |  ts BIGINT,
      |  procTime AS PROCTIME(),
      |  eventTime AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000, 'yyyy-MM-dd HH:mm:ss')),
      |  WATERMARK FOR eventTime AS eventTime - INTERVAL '10' SECOND
      | ) WITH (
      |  'connector.type' = 'kafka',
      |  'connector.version' = 'universal',
      |  'connector.topic' = 'ng_log_par_extracted',
      |  'connector.startup-mode' = 'latest-offset',
      |  'connector.properties.zookeeper.connect' = 'hadoop02:2181,hadoop03:2181,hadoop04:2181',
      |  'connector.properties.bootstrap.servers' = 'hadoop04:9092,hadoop05:9092,hadoop06:9092',
      |  'connector.properties.group.id' = 'rtdw_ng_log_par_extracted',
      |  'format.type' = 'json',
      |  'format.derive-schema' = 'true',
      |  'update-mode' = 'append'
      | )
    """.stripMargin
    tableEnv.sqlUpdate(createTableSql)

    //create table不支持if not exists，先判断是否存在表，存在删除
    val dropSinkTable = "DROP TABLE IF EXISTS rtdw.flink_ods.pvuv_sink"
    tableEnv.sqlUpdate(dropSinkTable)

    //创建和Mysql端连接的Hive表，是一个外部连接关系，真实的mysql表是通过connector.table参数指定
    //需要提前创建好
    val createMysql =
    """
      |CREATE TABLE rtdw.flink_ods.pvuv_sink (
      |    eventType STRING,
      |	   windowStart timestamp(3),
      |	   windowEnd timestamp(3),
      |    pv BIGINT,
      |    uv BIGINT
      |) WITH (
      |    'connector.type' = 'jdbc',
      |    'connector.url' = 'jdbc:mysql://192.168.7.105:3306/flink_test',
      |    'connector.table' = 'pvuv_sink',
      |    'connector.username' = 'root',
      |    'connector.password' = 'steve201718',
      |    'connector.write.flush.max-rows' = '5'
      |)
    """.stripMargin
    tableEnv.sqlUpdate(createMysql)

    //查询数据
    //查询出的数据格式
    //(add,2020-05-27T15:54:30,2020-05-27T15:55,4,3)
    //(buy,2020-05-27T15:55,2020-05-27T15:55:30,9,3)
    val querySql =
    """
      |SELECT eventType,
      |   TUMBLE_START(eventTime, INTERVAL '30' SECOND) AS windowStart,
      |   TUMBLE_END(eventTime, INTERVAL '30' SECOND) AS windowEnd,
      |   COUNT(userId) AS pv,
      |   COUNT(DISTINCT userId) AS uv
      | FROM rtdw.flink_ods.streaming_user_active_log
      | GROUP BY eventType,TUMBLE(eventTime, INTERVAL '30' SECOND)
    """.stripMargin
    val resultTable = tableEnv.sqlQuery(querySql)
    //resultTable.toRetractStream[Row].print().setParallelism(1)
    //tableEnv.toRetractStream[Row](resultTable).print()
    //直接在TM上输出数据
    //tableEnv.toAppendStream[Row](resultTable).print()

    //将数据输出到自定义的Sink表中，会自动同步到Mysql中
    val insertSql =
      """
        |INSERT INTO rtdw.flink_ods.pvuv_sink
        |SELECT eventType,
        |   TUMBLE_START(eventTime, INTERVAL '30' SECOND) AS windowStart,
        |   TUMBLE_END(eventTime, INTERVAL '30' SECOND) AS windowEnd,
        |   COUNT(userId) AS pv,
        |   COUNT(DISTINCT userId) AS uv
        | FROM rtdw.flink_ods.streaming_user_active_log
        | GROUP BY eventType,TUMBLE(eventTime, INTERVAL '30' SECOND)
      """.stripMargin

    tableEnv.sqlUpdate(insertSql)

    val kudu_master: String = "192.168.7.111:7051"
    val kudu_batch: Int = 5

    //数据插入到kudu表中
    tableEnv.toAppendStream[Row](resultTable).addSink(new SinkPVToKudu(kudu_master, kudu_batch)).setParallelism(1)

    //val query = "SELECT eventType,userId,platform,ts FROM rtdw.flink_ods.streaming_user_active_log"
    //val result = tableEnv.sqlQuery(query)
    //result.toRetractStream[Row].print().setParallelism(1)

    streamEnv.execute("Flink SQL PV Demo")
  }
}
