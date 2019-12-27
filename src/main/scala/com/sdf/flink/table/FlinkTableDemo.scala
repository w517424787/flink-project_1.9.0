package com.sdf.flink.table

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._

object FlinkTableDemo {

  case class Person(userid: String, product: String, amount: Int)

  def main(args: Array[String]): Unit = {

    //批处理表
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = BatchTableEnvironment.create(env)

    //流式表
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val streamTableEnv = StreamTableEnvironment.create(streamEnv, fsSettings)

    //    val orderDS = env.fromElements(("1001", "A001", 3000), ("1001", "A001", 6000), ("1002", "A001", 6000))
    //      .map(line => Person(line._1, line._2, line._3))
    val path = this.getClass.getClassLoader.getResource("worddemo.txt").getPath
    val orderDS = env.readTextFile(path)
      .map(line => {
        val array = line.split(",")
        Person(array(0), array(1), array(2).toInt)
      })
    //需要引入 org.apache.flink.table.api.scala._
    tableEnv.registerDataSet("Person", orderDS, 'userid, 'product, 'amount)
    val result = tableEnv.sqlQuery("select t.userid,sum(t.amount) as total_amount from Person t group by t.userid")
    val rows = result.collect()
    println(rows)
  }
}
