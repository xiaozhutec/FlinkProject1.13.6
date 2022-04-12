package com.johngo.streaming.transformation;


import com.johngo.streaming.customeSource.JGRichParallelSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Johngo
 * @date 2022/4/11
 *
 * 接收来自 JGParallelSource 的数据流，过滤奇数项
 */

public class JGStreamingFilter {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> text = env.addSource(new JGRichParallelSource()).setParallelism(1);
        SingleOutputStreamOperator<Long> mapData = text.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接收到的原始数据是：" + value);
                return value;
            }
        });
        SingleOutputStreamOperator<Long> filterData = mapData.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                if (value%2 == 0) {
                    System.out.println("被保留：" + value);
                    return true;
                } else {
                    System.out.println("被舍弃：" + value);
                    return false;
                }
            }
        });
        filterData.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("过滤之后的数据是：" + value);
                return value;
            }
        });

        env.execute(JGStreamingFilter.class.getSimpleName());
    }
}
