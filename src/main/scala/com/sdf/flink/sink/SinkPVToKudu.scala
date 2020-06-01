package com.sdf.flink.sink

import com.sdf.flink.kudu.FlinkPVIntoKudu
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.types.Row
import org.apache.kudu.client.{KuduClient, KuduSession, Operation, SessionConfiguration}

/**
  * Modify by wwg 2020-05-28
  * 将数据插入到Kudu中
  *
  * @param kudu_master kudu master
  * @param kudu_batch  batch
  */

class SinkPVToKudu(kudu_master: String, kudu_batch: Int) extends RichSinkFunction[Row] {

  private var kudu_client: KuduClient = _
  private var kudu_session: KuduSession = _
  private var operation_batch: IntCounter = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    //判断kudu连接
    if (kudu_client == null || kudu_session == null) {
      //创建kudu连接
      getKuduConnection()
    }
  }


  override def close(): Unit = {
    super.close()
    //关闭连接资源
    if (kudu_session != null) {
      kudu_session.close()
    }
    if (kudu_client != null) {
      kudu_client.close()
    }
  }


  override def invoke(value: Row, context: SinkFunction.Context[_]): Unit = {
    var operation: Operation = null
    if (value != null) {
      operation = FlinkPVIntoKudu.insertPVIntoKudu(value.toString, kudu_client)
      if (operation != null) {
        kudu_session.apply(operation)
        //TODO:这里需要按批次分配插入到kudu表中，测试则直接插入数据
        kudu_session.flush()
      }
    }
  }

  /**
    * 创建kudu连接
    */
  def getKuduConnection() {
    operation_batch = new IntCounter()
    //创建kudu连接
    kudu_client = new KuduClient.KuduClientBuilder(kudu_master).build()
    kudu_session = kudu_client.newSession()
    kudu_session.setTimeoutMillis(300000)
    kudu_session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH)
    kudu_session.setMutationBufferSpace(kudu_batch)
  }
}
