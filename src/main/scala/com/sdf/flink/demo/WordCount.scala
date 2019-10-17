package com.sdf.flink.demo

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.configuration.{ConfigConstants, Configuration}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)

    val env = ExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    val text = env.readTextFile("C:\\FlinkProject\\flink-project_1.9.0\\data\\test.txt")
    val counts = text.flatMap(_.split(",")).map((_, 1))
      .groupBy(0)
      .sum(1)
      .map(x => (1, x._2, x))
      .groupBy(0)
      .sortGroup(1, Order.DESCENDING)
      .first(10)
      .map(x => x._3)

    counts.print()
  }
}
