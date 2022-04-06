package com.johngo.streaming.customeSource;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Johngo
 * @date 2022/4/2
 */

public class StreamingWithJGRichParallelSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> text = env.addSource(new JGRichParallelSource());
        text.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                if (value == 2) {
                    throw new RuntimeException();
                }
                else
                    return value;
            }
        });
        text.print();
        env.execute(StreamingWithJGNoParallelSource.class.getSimpleName());
    }
}
