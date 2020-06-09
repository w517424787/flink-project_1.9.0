package com.sdf.flink.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

import scala.collection.mutable

/**
 * Modify by wwg 2020-06-09
 * Flink Lateral Join UDAF
 * 用户自定义函数
 */

object FlinkLateralJoinDemo {

  case class Users(name: String, age: Int)

  class SplitUDF extends TableFunction[Users] {
    def eval(user: String): Unit = {
      //数据格式：A#B
      if (user != "") {
        val arrays = user.split("#")
        collect(Users(arrays(0), arrays(1).toInt))
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(streamEnv, settings)
    streamEnv.setParallelism(1)

    val usersData = List("Sunny#8", "Kevin#36", "Panpan#36")
    //streamEnv.fromCollection(usersData).print()
    val dataStream = streamEnv.fromCollection(usersData)
    val userTable = dataStream.toTable(tableEnv, 'data)
    tableEnv.createTemporaryView("Users", userTable)
    tableEnv.registerFunction("SplitUDF", new SplitUDF())

    //查询语句
    val query = "SELECT data,name,age FROM Users,LATERAL TABLE(SplitUDF(data)) AS T(name,age)"
    tableEnv.sqlQuery(query).toAppendStream[Row].print()

    streamEnv.execute("FlinkLateralJoinDemo")
  }
}
