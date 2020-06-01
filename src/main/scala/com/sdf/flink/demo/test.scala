package com.sdf.flink.demo

import java.text.SimpleDateFormat
import java.util.Date

object test {
  def main(args: Array[String]): Unit = {
    //println(this.getClass.getClassLoader.getResource("worddemo.txt").getPath)
    val time = "2020-05-29T11:09:00"
    val date1: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm")
    val date2: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date: Date = date1.parse(time)
    println(date2.format(date))
    //16
    println(time.length)
  }
}
