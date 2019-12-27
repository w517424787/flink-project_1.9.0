package com.sdf.flink.table

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.Table
import org.apache.flink.table.sinks.CsvTableSink

object TableSourceDemo {
  def main(args: Array[String]): Unit = {

    //批处理表
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = BatchTableEnvironment.create(env)

    val path = this.getClass.getClassLoader.getResource("sourcedemo.csv").getPath

    val fieldNames: Array[String] = Array("name", "age", "address")
    val fieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.INT, Types.STRING)
    val csvSource: CsvTableSource = new CsvTableSource(path, fieldNames, fieldTypes, ",", "\n", null, true, null, false)

    tableEnv.registerTableSource("person", csvSource)

    //tableEnv.sqlQuery("select * from person").printSchema()

    //val rows = tableEnv.sqlQuery("select name,sum(age) as age from person group by name").collect()
    //println(rows)

    //sink
    //val sinkPath = this.getClass.getClassLoader.getResource("csvsinkdemo.csv").getPath
    val csvSink: CsvTableSink = new CsvTableSink("E:\\flink-demo\\flink-project_1.9.0\\data\\test.csv", ",", 1, WriteMode.OVERWRITE)
    //define the field names and types
    val sinkFieldNames: Array[String] = Array("name", "age")
    val sinkFieldTypes: Array[TypeInformation[_]] = Array(Types.STRING, Types.INT)
    // register the TableSink as table "CsvSinkTable"

    tableEnv.registerTableSink("CsvSinkTable", sinkFieldNames, sinkFieldTypes, csvSink)
    //tableEnv.sqlUpdate("insert into CsvSinkTable select name,sum(age) as age from person group by name")
    val result: Table = tableEnv.sqlQuery("select name,sum(age) as age from person group by name")
    //插入数据到外部Csv中
    result.insertInto("CsvSinkTable")

    //result.printSchema()
    tableEnv.execute("job")
  }
}
