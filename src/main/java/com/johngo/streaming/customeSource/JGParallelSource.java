package com.johngo.streaming.customeSource;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * @author Johngo
 * @date 2022/4/2
 */

public class JGParallelSource implements ParallelSourceFunction<Long> {
    private Long count = 1L;
    private boolean isRunning = true;

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
