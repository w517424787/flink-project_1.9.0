package com.sdf.flink.demo

import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

/**
 * Modify by wwg 2020-06-22
 * scala jdbc通用操作
 * https://www.cnblogs.com/xuziyu/p/10799202.html
 */
object ScalikeJDBCDemo {

  case class PV_UV(eventType: String, windowStart: String, windowEnd: String, pv: BigInt, uv: BigInt)

  def main(args: Array[String]): Unit = {
    DBs.setup()

    // 读取使用的是readOnly
    val pv: List[PV_UV] = DB.readOnly(implicit session => {
      SQL("select eventType,windowStart,windowEnd,pv,uv from pvuv_sink where windowStart = '2020-05-29 11:08:00'")
        .map(rs => {
          PV_UV(rs.string("eventType"), rs.string("windowStart"),
            rs.string("windowEnd"), rs.bigInt("pv"), rs.bigInt("uv"))
        }).list().apply()
    })

    //输出数据
    pv.foreach(p => {
      println(p.eventType + "|" + p.windowStart + "|" + p.windowEnd + "|" + p.pv + "|" + p.uv)
    })
  }
}
