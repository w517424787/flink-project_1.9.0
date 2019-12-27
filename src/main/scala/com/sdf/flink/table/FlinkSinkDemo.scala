package com.sdf.flink.table

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

object FlinkSinkDemo {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //val settings = EnvironmentSettings.newInstance.useOldPlanner.inStreamingMode.build
    //val tEnv = TableEnvironment.create(settings)
    env.setParallelism(1)
    val tEnv = BatchTableEnvironment.create(env)

    val fieldNames = Array("name", "age", "address")
    val fieldTypes = Array[TypeInformation[_]](Types.STRING, Types.INT, Types.STRING)

    val path = this.getClass.getClassLoader.getResource("sourcedemo.csv").getPath
    tEnv.registerTableSource("person", new CsvTableSource(path, fieldNames, fieldTypes, ",", "\n", null, true, null, false))

    //    val persons = tEnv.scan("person")
    //    val result = persons.groupBy('name)
    //      .select('name,'age.sum as 'total_sum)
    //      .toDataSet[Row].print()

    val sinkPath = this.getClass.getClassLoader.getResource("csvsinkdemo.csv").getPath
    //tEnv.registerTableSink("MySink1", new CsvTableSink(sinkPath, ",", 1, WriteMode.OVERWRITE).configure(fieldNames, fieldTypes))

    //val table: Table = tEnv.sqlQuery("select name,age,address from MySource1")
    //table.insertInto("MySink1")
    //println(table.collect())

    //tEnv.execute("job")
  }
}
