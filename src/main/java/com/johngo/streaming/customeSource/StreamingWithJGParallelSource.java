package com.johngo.streaming.customeSource;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Johngo
 * @date 2022/4/2
 */

public class StreamingWithJGParallelSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> text = env.addSource(new JGParallelSource());
        text.print();
        env.execute(StreamingWithJGNoParallelSource.class.getSimpleName());
    }
}
