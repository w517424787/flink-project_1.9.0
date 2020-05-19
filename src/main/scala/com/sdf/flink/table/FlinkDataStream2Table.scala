package com.sdf.flink.table

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object FlinkDataStream2Table {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val streamTableEnv = StreamTableEnvironment.create(streamEnv, settings)
    //val env = ExecutionEnvironment.getExecutionEnvironment
    //val tEnv = BatchTableEnvironment.create(env)

    val ds = streamEnv.fromElements("a", "b", "c")
    ds.toTable(streamTableEnv, 'f1, 'proctime.proctime).toAppendStream[Row].print()

    streamEnv.execute("test")

    //获取数据集
    //val ds: DataSet[(String, Int)] = env.fromElements(("Tom", 20), ("Sunny", 30), ("Sunny", 20))

    //val table: Table = tEnv.fromDataSet(ds, 'name, 'age)

    //table转换为dataset
    //val dsRow: DataSet[Row] = tEnv.toDataSet[Row](table)

    //table.printSchema()

    //val result = table.groupBy('name).select('name, 'age.avg as 'avg_age).collect()
    //println(result)

    //注册流数据表，不指定字段，默认字段：f0,f1
    //streamTableEnv.registerDataStream("person", stream)
    //指定字段
    //streamTableEnv.registerDataStream("person", stream, 'name, 'age)

    //val result = streamTableEnv.sqlQuery("select * from person").collect()
    //println(result)
  }
}
