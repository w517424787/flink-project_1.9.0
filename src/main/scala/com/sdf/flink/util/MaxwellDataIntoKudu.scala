package com.sdf.flink.util

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kudu.ColumnSchema
import org.apache.kudu.client.{KuduClient, Operation}
import scala.collection.JavaConversions._

/**
  * 批量解析json数据，存储到Kudu表中
  */

object MaxwellDataIntoKudu extends Logger {

  /** {"appId":9999,"id":17012285,"playerId":1000001,"collectTime":"2019-12-10 14:28:08",
    * "tableName":"dwm.m_f_hall_buried_point_stat",
    * "clickType":"20001.3.1.1.7","devicecode":"DESKTOP-OIH0F6Nxaasa",
    * "serverVersion":"TYF","os":4,"param":"2002.1"}
    *
    * @param jsonStr 上报的log数据
    * @return
    */
  def getKuduOperation(jsonStr: String, kudu_client: KuduClient): Operation = {
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
