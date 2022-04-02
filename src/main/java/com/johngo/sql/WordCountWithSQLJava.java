package com.johngo.sql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


import java.util.ArrayList;

/**
 * @author Johngo
 * @date 2022/4/1
 */

public class WordCountWithSQLJava {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建一个数据集合
        String names = "Flink,Spark,Hadoop,Spark,Flink,FLink,Scala";
        ArrayList<WC> name = new ArrayList<>();
        String[] splits = names.split(",");
        for(String split: splits) {
            name.add(new WC(split, 1L));
        }

        DataStreamSource<WC> dataStream = env.fromCollection(name);
        //DataSet 转sql, 指定字段名
        Table inputTable = tableEnv.fromDataStream(dataStream).as("name", "cnt");
        inputTable.printSchema();

        // 注册表为视图 & 查询
        tableEnv.createTemporaryView("InputTable", inputTable);
        Table resultTable = tableEnv.sqlQuery("SELECT name, SUM(cnt) FROM InputTable GROUP BY name");

        // 将结果数据转换为DataStream
        DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);

        resultStream.print();
        env.execute();
    }


    public static class WC {
        public String word;
        public long count;

        public  WC() {}
        public WC(String word, long count) {
            this.word = word;
            this.count = count;
        }
        @Override
        public String toString() {
            return "WordWithCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
