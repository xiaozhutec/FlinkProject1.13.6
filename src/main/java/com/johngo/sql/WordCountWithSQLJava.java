package com.johngo.sql;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;


/**
 * @author Johngo
 * @date 2022/4/1
 */

public class WordCountWithSQLJava {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<WC> dataStream = env.socketTextStream("localhost", 8899)
                .flatMap(new FlatMapFunction<String, WC>() {
            @Override
            public void flatMap(String value, Collector<WC> out) throws Exception {
                String[] splits = value.split(",");
                for (String split : splits) {
                    out.collect(new WC(split, 1L));
                }
            }
        });

        //DataStream 转sql & 查询
        Table WordCountTable = tableEnv.fromDataStream(dataStream);
        WordCountTable.printSchema();
        tableEnv.createTemporaryView("WC", WordCountTable);

        Table resultTable = tableEnv.sqlQuery("SELECT word, SUM(`count`) FROM WC group by word");

        // 将结果数据转换为DataStream toRetractStream toAppendStream
        tableEnv.toRetractStream(resultTable, Row.class).print().setParallelism(1);
        env.execute("WCSQLJava");
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
            return "WC {" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
