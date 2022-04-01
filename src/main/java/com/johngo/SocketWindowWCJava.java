package com.johngo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Flink 对数据进行wc计算
 *
 * 滑动窗口计算（每隔1秒对最近2秒内的数据进行累加）
 * @author Johngo
 * @date 2022/4/1
 */

public class SocketWindowWCJava {
    public static void main(String[] args) throws Exception {
        // 获取流式运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String hostname = "localhost";
        int port = 8899;
        String delimiter = "\n";
        // 获取数据源(Socket数据源,单词以逗号分割)
        DataStreamSource<String> source = env.socketTextStream(hostname, port, delimiter);
        SingleOutputStreamOperator<WC> res = source.flatMap(new FlatMapFunction<String, WC>() {

                    @Override
                    public void flatMap(String value, Collector<WC> out) throws Exception {
                        String[] splits = value.split(",");
                        for (String split : splits) {
                            out.collect(new WC(split, 1));
                        }
                    }
                }).keyBy(x -> x.word)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(1), Time.seconds(2)))  // 每隔1秒，统计过去2秒的数据
                // .sum("count");
                .reduce(new ReduceFunction<WC>() {
                    @Override
                    public WC reduce(WC t1, WC t2) throws Exception {
                        return new WC(t1.word, t1.count+t2.count);
                    }
                });

        res.print().setParallelism(1);

        env.execute("SocketWindowWCJava");
    }

    public static class WC {
        public String word;
        public int count;

        public WC() {}
        public WC(String word, int count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
