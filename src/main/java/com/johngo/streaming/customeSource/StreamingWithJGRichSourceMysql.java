package com.johngo.streaming.customeSource;

import com.johngo.domain.Person;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Johngo
 * @date 2022/4/8
 */

public class StreamingWithJGRichSourceMysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Person> text = env.addSource(new JGRichSourceMysql()).setParallelism(1);

        text.print("PersonInfo:");

        env.execute("StreamingWithJGRichSourceMysql");
    }
}
