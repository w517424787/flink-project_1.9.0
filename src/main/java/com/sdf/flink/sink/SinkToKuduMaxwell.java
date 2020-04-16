package com.sdf.flink.sink;

import com.sdf.flink.util.GetJSONDataIntoKudu;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.SessionConfiguration;

import java.util.List;

/**
 * Modify by wwg 2020-04-13
 * 将maxwell同步的mysql binlog数据插入到Kudu中
 */

public class SinkToKuduMaxwell extends RichSinkFunction<String> {
    private List<String> kudu_master;
    private int kudu_batch;
    private static KuduClient kudu_client;
    private static KuduSession kudu_session;
    private IntCounter operation_batch = null; //条数计数器

    /**
     * 获取kudu master地址和每批次插入数据的笔数
     *
     * @param kudu_master kudu master地址
     * @param kudu_batch  每批次插入的笔数
     */
    public SinkToKuduMaxwell(List<String> kudu_master, int kudu_batch) {
        this.kudu_master = kudu_master;
        this.kudu_batch = kudu_batch;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (kudu_client == null || kudu_session == null) {
            //创建kudu连接
            getKuduConnection();
        }
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        if (value != null && !"".equals(value)) {
            //获取插入kudu表operation
            Operation operation = GetJSONDataIntoKudu.getMaxwellKuduOperation(value, kudu_client);
            if (operation != null) {
                kudu_session.apply(operation);
                this.operation_batch.add(1);
                kudu_session.flush();

                //测试
                //if (this.operation_batch.getLocalValue() > this.kudu_batch / 2) {
                /*if (this.operation_batch.getLocalValue() > 10) {
                    kudu_session.flush();
                    this.operation_batch.resetLocal();
                    // 确保数据插入成功
                    if (this.operation_batch.getLocalValue() > 0) {
                        kudu_session.flush();
                    }
                }*/
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (kudu_session != null) {
            kudu_session.close();
        }
        if (kudu_client != null) {
            kudu_client.close();
        }
    }

    /**
     * 获取Kudu连接
     */
    private void getKuduConnection() {
        operation_batch = new IntCounter();
        //创建kudu连接
        kudu_client = new KuduClient.KuduClientBuilder(kudu_master).build();
        kudu_session = kudu_client.newSession();
        kudu_session.setTimeoutMillis(300000);
        kudu_session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        kudu_session.setMutationBufferSpace(kudu_batch);
    }
}
