package com.sdf.flink.demo

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
  * 验证下数据倾斜情况
  */
object FlinkRebalanceDemo {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val ds = env.generateSequence(1, 300000)
    val rebalanced = ds.filter(_ > 500).rebalance()

    val countsInPartition = rebalanced.map(new RichMapFunction[Long, (Int, Long)] {
      override def map(value: Long): (Int, Long) =
        (getRuntimeContext.getIndexOfThisSubtask, 1)
    }).groupBy(0).sum(1)

    countsInPartition.print()
  }
}
