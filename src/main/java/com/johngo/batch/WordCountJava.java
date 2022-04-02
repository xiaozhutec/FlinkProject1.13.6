package com.johngo.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author Johngo
 * @date 2022/4/1
 */

public class WordCountJava {
    public static void main(String[] args) throws Exception {
        String filePath = "./datas/dm.csv";
        String resultPath = "./datas/wc_rst.csv";

        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> text = env.readTextFile(filePath);

        AggregateOperator<Tuple2<String, Integer>> res = text.flatMap(new JGFlatMapFunction())
                .groupBy(0)
                .sum(1);
        res.print();
        res.writeAsCsv(resultPath).setParallelism(1);

        env.execute("WordCountJava");
    }

    public static class JGFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] splits = value.split(",");
            for (String split : splits) {
                out.collect(Tuple2.of(split, 1));
            }
        }
    }
}
