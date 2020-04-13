package com.sdf.flink.util

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kudu.ColumnSchema
import org.apache.kudu.client.{KuduClient, Operation}
import scala.collection.JavaConversions._

/**
  * Modify by wwg 2020-04-13
  * 批量解析json数据，存储到Kudu表中
  */

object GetJSONDataIntoKudu extends Logger {

  /**
    * 解析maxwell同步mysql数据库的binlog，并将数据插入到Kudu中
    * json格式：
    * {"database":"test","table":"aaaa","type":"insert","ts":1546584409,"xid":75963,"commit":true,
    * "data":{"id":1003,"createDate":"2019-01-04 14:43:45"}}
    * {"database":"test","table":"aaaa","type":"update","ts":1546584757,"xid":99229,"commit":true,
    * "data":{"id":1004,"createDate":"2019-01-05 14:43:45"},"old":{"createDate":"2019-01-04 14:43:45"}}
    * {"database":"test","table":"aaaa","type":"delete","ts":1546593701,"xid":683967,"commit":true,
    * "data":{"id":1004,"createDate":"2019-01-05 14:43:45"}}
    * mysql表操作分insert、update、delete
    * 转成kudu操作就是upsert、delete
    * kudu表需要动态拼接，基础数据落地到kudu_ods层，如：impala::kudu_ods.log_t_redticket_record
    * 对于maxwell监控的表是按天分表的（如：t_currency_record_20181123），
    * kudu表存储的是t_currency_record，是不按时间进行拆分的
    *
    * @param jsonStr mysql的binlog
    * @param kudu_client kudu客户端
    * @return
    */
  def getMaxwellKuduOperation(jsonStr: String, kudu_client: KuduClient): Operation = {
    //解析JSON
    if (jsonStr != null && jsonStr.nonEmpty && jsonStr != "") {

      val jsonObject: JSONObject = JSON.parseObject(jsonStr)
      val database = jsonObject.getString("database")
      var table = jsonObject.getString("table")
      val operationType = jsonObject.getString("type")
      val data = jsonObject.getString("data")

      //判断表是否是按日期进行分表
      //格式：t_currency_record_yyyyMM,t_currency_record_yyyyMMdd
      //t_currency_record_20190105
      if (table.contains("_20")) {
        table = table.split("_20")(0)
      }

      //kudu表
      val kuduTableName = "impala::kudu_ods." + database + "_" + table
      log.info(s"kuduTableName:$kuduTableName")
      log.info(s"type:$operationType")

      //Modify by wwg 2020-02-18
      //需要判断kudu表是否存在，不存在直接跳过
      if (kudu_client.tableExists(kuduTableName)) {

        val kudu_table = kudu_client.openTable(kuduTableName)
        //kudu全部列名
        val kudu_columns = kudu_table.getSchema.getColumns
        var column_list = List[String]()
        for (column <- kudu_columns) {
          column_list = column_list :+ column.getName
        }
        var pkColumns: java.util.List[ColumnSchema] = null
        var list = List[String]()
        val operation: Operation = operationType.toLowerCase match {
          case "insert" | "update" => kudu_table.newUpsert()
          case "delete" =>
            //获取kudu表的主键列
            pkColumns = kudu_table.getSchema.getPrimaryKeyColumns
            for (column <- pkColumns) {
              list = list :+ column.getName
            }
            //后端会定期删除历史数据，释放空间，大数据这边不需要删除，默认给个无效值
            //kudu_table.newDelete()
            null
        }
        if (operation != null) {
          val row = operation.getRow

          // 解析data中的数据
          val dataObject: JSONObject = JSON.parseObject(data)
          val keys = dataObject.keySet()
          for (key <- keys) {
            //delete只能传表主键，不能添加普通列，否则无法删除数据
            if (operationType.toLowerCase.equals("delete")) {
              if (list.contains(key.toLowerCase)) {
                row.addString(key.toLowerCase, dataObject.getString(key))
              }
            } else {

              //判断kudu表是否存在该列
              if (column_list.contains(key.toLowerCase)) {
                row.addString(key.toLowerCase, dataObject.getString(key))
              }
            }
          }
        }

        operation
      }
      else {
        null
      }
    }
    else {
      null
    }
  }


  /** {"appId":9999,"id":17012285,"playerId":1000001,"collectTime":"2019-12-10 14:28:08",
    * "tableName":"dwm.m_f_hall_buried_point_stat","clickType":"20001.3.1.1.7",
    * "devicecode":"DESKTOP-OIH0F6Nxaasa","serverVersion":"TYF","os":4,"param":"2002.1"}
    *
    * @param jsonStr 埋点上报的log数据
    * @return
    */
  def getBuriedKuduOperation(jsonStr: String, kudu_client: KuduClient): Operation = {
    //解析JSON
    if (jsonStr != null && jsonStr.nonEmpty && jsonStr != "") {

      //判断json格式是否符合要求
      try {
        val jsonObject: JSONObject = JSON.parseObject(jsonStr)
        if (jsonObject.containsKey("tableName")) {
          val tableName = jsonObject.getString("tableName")
          val kuduTableName = "impala::" + tableName
          //log.info(s"kuduTableName:$kuduTableName")

          //判断kudu表是否存在,不存在直接退出
          if (kudu_client.tableExists(kuduTableName)) {
            val kudu_table = kudu_client.openTable(kuduTableName)

            //判断传送的json中列是否存在
            //kudu全部列名
            val kudu_columns = kudu_table.getSchema.getColumns
            //==========================================
            //存储表主键
            //Modify by WWG 2019-06-19
            var pkColumns: java.util.List[ColumnSchema] = null
            var pkList = List[String]()
            pkColumns = kudu_table.getSchema.getPrimaryKeyColumns
            for (column <- pkColumns) {
              pkList = pkList :+ column.getName
            }
            //===========================================
            var column_list = List[String]()
            for (column <- kudu_columns) {
              column_list = column_list :+ column.getName
            }

            val operation: Operation = kudu_table.newUpsert()
            val row = operation.getRow
            //json中的字段名称
            val keys = jsonObject.keySet()
            for (key <- keys) {

              //判断列是否存在
              if (column_list.contains(key.toLowerCase)) {
                //判断主键内容是否为空
                if (pkList.contains(key.toLowerCase)) {
                  if ("" == jsonObject.getString(key) || "null" == jsonObject.getString(key).toLowerCase) {
                    row.addString(key.toLowerCase, "9999") //给主键默认值9999
                  } else {
                    row.addString(key.toLowerCase, jsonObject.getString(key))
                  }
                } else {
                  row.addString(key.toLowerCase, jsonObject.getString(key))
                }
              }
            }

            //系统插入当前时间
            row.addString("createtime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))

            operation
          } else {
            log.info(s"kuduTableName:$kuduTableName is not exist!")
            null
          }
        } else {
          null
        }
      } catch {
        case e: Exception => log.info("json格式不正确," + e.printStackTrace())
          null
      }
    }
    else {
      null
    }
  }
}
