package com.sdf.flink.state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
//添加隐式引用
import org.apache.flink.streaming.api.scala._

object CountWindowState2 {

  class CountWindowAverage extends RichFlatMapFunction[(Long, Long), (Long, Long)] {

    private var sum: ValueState[(Long, Long)] = _

    override def flatMap(input: (Long, Long), out: Collector[(Long, Long)]): Unit = {

      //获取当前状态值
      val currentSum = sum.value() match {
        case null => (0L, 0L)
        case _ => sum.value()
      }

      //更新值
      val newSum = (currentSum._1 + 1, currentSum._2 + input._2)
      //更新状态
      sum.update(newSum)

      if (newSum._1 >= 2) {
        out.collect((input._1, newSum._2 / newSum._1))
        sum.clear()
      }
    }

    override def open(parameters: Configuration): Unit = {

      //设置状态过期参数
      val ttlConfig = StateTtlConfig.newBuilder(Time.seconds(10))
        //.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        //.setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
        .updateTtlOnCreateAndWrite()
        .returnExpiredIfNotCleanedUp()
        .cleanupInBackground()
        .build()

      val valueStateDescriptor = new ValueStateDescriptor[(Long, Long)]("average",
        TypeInformation.of(new TypeHint[(Long, Long)]() {}))

      valueStateDescriptor.enableTimeToLive(ttlConfig)

      sum = getRuntimeContext.getState(valueStateDescriptor)
    }
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //需要添加隐式引用 org.apache.flink.streaming.api.scala._
    env.fromCollection(List((1L, 3L), (1L, 5L), (2L, 7L), (2L, 5L), (1L, 2L), (1L, 8L)))
      .keyBy(_._1)
      .flatMap(new CountWindowAverage())
      .print()

    env.execute("state demo")
  }
}
