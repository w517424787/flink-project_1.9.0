package com.sdf.flink.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

public class WordCountTable {
    public static void main(String[] args) throws Exception {
        //批处理
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        //流
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tbEnv = StreamTableEnvironment.create(executionEnvironment);

        //String path = WordCountTable.class.getClassLoader().getResource("words.txt").getPath();
        //System.out.println(path);

        String path = "E:\\flink-project\\flink-project_1.9.0\\data\\words.txt";

        //注册表（方式一）
        tEnv.connect(new FileSystem().path(path)).withFormat(new OldCsv().field("name", Types.STRING)
                        .field("score", Types.INT).fieldDelimiter(",").lineDelimiter("\n"))
                .withSchema(new Schema().field("name", Types.STRING).field("score", Types.INT))
                .registerTableSource("person");

        //注册表（方式二）
        TableSource tableSource = new CsvTableSource(path, new String[]{"name", "score"}, new TypeInformation[]{Types.STRING, Types.INT});
        tEnv.registerTableSource("person2", tableSource);

        Table result2 = tEnv.sqlQuery("select name,sum(score) as total_score from person2 group by name");
        tEnv.toDataSet(result2, Row.class).print();



        //返回表
//        Table result = tEnv.scan("person")
//                .groupBy("name")
//                .select("name,sum(score) as total_score");

        //Table result = tEnv.sqlQuery("select name,sum(score) as total_score from person group by name");

        //输出表数据
        //tEnv.toDataSet(result, Row.class).print();


    }
}
