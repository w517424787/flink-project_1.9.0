package com.sdf.flink.ml;

import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.batch.source.CsvSourceBatchOp;
import com.alibaba.alink.pipeline.Pipeline;
import com.alibaba.alink.pipeline.clustering.KMeans;
import com.alibaba.alink.pipeline.dataproc.vector.VectorAssembler;

public class KMeansDemo {
    public static void main(String[] args) throws Exception {
        final String URL = "E:\\flink-demo\\flink-project_1.10.0\\data\\iris.csv";
        final String SCHEMA_STR = "sepal_length double, sepal_width double, petal_length double," +
                " petal_width double, category string";

        //读取数据
        BatchOperator data = new CsvSourceBatchOp().setFilePath(URL).setSchemaStr(SCHEMA_STR);
        VectorAssembler vector = new VectorAssembler()
                .setSelectedCols(new String[]{"sepal_length", "sepal_width", "petal_length", "petal_width"})
                .setOutputCol("features");

        KMeans kMeans = new KMeans().setVectorCol("features").setK(3)
                .setPredictionCol("prediction_result")
                .setPredictionDetailCol("prediction_detail")
                .setReservedCols("category")
                .setMaxIter(100);

        Pipeline pipeline = new Pipeline().add(vector).add(kMeans);
        pipeline.fit(data).transform(data).print();
    }
}
