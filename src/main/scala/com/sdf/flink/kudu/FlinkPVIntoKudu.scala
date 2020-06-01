package com.sdf.flink.kudu

import java.text.SimpleDateFormat
import java.util.Date

import com.sdf.flink.util.Logger
import org.apache.kudu.client.{KuduClient, Operation}

/**
  * Modify by wwg 2020-05-29
  * flink sql计算pv、uv，并将其插入到Kudu表中
  */
object FlinkPVIntoKudu extends Logger {

  /**
    * 插入数据到kudu中
    *
    * @param value (buy,2020-05-27T15:55,2020-05-27T15:55:30,9,3)
    * @param kudu_client
    * @return
    */
  def insertPVIntoKudu(value: String, kudu_client: KuduClient): Operation = {
    val kuduTableName = "impala::tmp.tmp_flink_pv_stat"
    //判断表是否存在
    if (kudu_client.tableExists(kuduTableName)) {
      val kudu_table = kudu_client.openTable(kuduTableName)
      val operation: Operation = kudu_table.newUpsert()
      val row = operation.getRow
      val arrays = value.split(",")
      //TODO:需要根据表中字段进行数据插入，这里只测试用
      row.addString("eventtype", arrays(0))
      //将日期格式归整化
      //日期格式存在:2020-05-27T15:55,2020-05-27T15:55:05
      var windowStart = arrays(1)
      if (windowStart.length == 16) {
        windowStart = windowStart + ":00"
      }
      var windowEnd = arrays(2)
      if (windowEnd.length == 16) {
        windowEnd = windowEnd + ":00"
      }
      //val date1: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
      //val date2: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      //val date: Date = date1.parse(time)
      //println(date2.format(date))
      row.addString("windowstart", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        .format(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(windowStart)))
      row.addString("windowend", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        .format(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(windowEnd)))
      row.addLong("pv", arrays(3).toLong)
      row.addLong("uv", arrays(4).toLong)

      //log.info("operation!")

      operation
    } else {
      null
    }
  }
}
