package com.sdf.flink.demo

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object WordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile("E:\\flink-demo\\flink-project_1.10.0\\data\\test.txt")
    val counts = text.flatMap(_.toLowerCase.split(",")).map((_,1)).keyBy(0).sum(1)
    counts.print()

    println(env.getExecutionPlan)

    env.execute("demo")
  }
}
