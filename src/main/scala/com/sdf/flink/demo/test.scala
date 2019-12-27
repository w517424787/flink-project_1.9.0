package com.sdf.flink.demo

object test {
  def main(args: Array[String]): Unit = {
    println(this.getClass.getClassLoader.getResource("worddemo.txt").getPath)
  }
}
