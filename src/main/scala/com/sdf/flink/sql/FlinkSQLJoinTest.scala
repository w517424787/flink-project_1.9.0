package com.sdf.flink.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object FlinkSQLJoinTest {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(streamEnv, settings)
    streamEnv.setParallelism(1)

    val data1 = List(3, 4, 5, 6, 7, 8, 9)
    val data2 = List((4, 7))
    val table1 = streamEnv.fromCollection(data1).toTable(tableEnv, 'num)
    val table2 = streamEnv.fromCollection(data2).toTable(tableEnv, 'startNum, 'endNum)
    tableEnv.createTemporaryView("a_table", table1)
    tableEnv.createTemporaryView("b_Table", table2)

    val query = "SELECT a.* FROM a_table AS a INNER JOIN b_Table AS b ON a.num > b.startNum AND a.num < b.endNum"
    tableEnv.sqlQuery(query).toRetractStream[Row].print()
    streamEnv.execute("FlinkSQLJoinTest")
  }
}
