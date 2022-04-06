package com.johngo.streaming.customeSource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @author Johngo
 * @date 2022/4/2
 */

public class JGRichParallelSource extends RichParallelSourceFunction<Long> {

    private Long count = 1L;
    private boolean isRunning = true;

    /**
     * 该方法在开始后，会被调用一次，用于获取数据源
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("打开数据源链接...");
    }

    /**
     * 该方法在程序关闭时，调用
     */
    @Override
    public void close() throws Exception {
        System.out.println("关闭数据源链接...");
    }

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(count);
            count += 1;
            Thread.sleep(1000); // 每1秒产生一条数据
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
