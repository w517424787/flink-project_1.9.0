package com.sdf.flink.demo

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment

object ExecutionPlanDemo {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.fromElements(
      "Who's there?",
      "I think I hear them. Stand, ho! Who's there?")

    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1).setParallelism(1)

    counts.writeAsText("E:\\flink-demo\\flink-project_1.10.0\\data\\test2.txt")

    println(env.getExecutionPlan())

    counts.print()
  }
}
