package com.johngo.streaming.customeSource

import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
 * @author Johngo
 * @date 2022/4/6
 */

class JGNoParallelSourceScala extends SourceFunction[Long] {
  var count = 1L
  var isRunning = true

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {

    while (isRunning) {
      ctx.collect(count)
      count += 1
      Thread.sleep(1000)
    }

  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
