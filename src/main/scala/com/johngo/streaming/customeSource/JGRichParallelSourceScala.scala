package com.johngo.streaming.customeSource

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

/**
 * @author Johngo
 * @date 2022/4/6
 */

private class JGRichParallelSourceScala extends RichParallelSourceFunction[Long]{
  var count = 1L
  var isRunning = true

  override def open(parameters: Configuration): Unit = {
    println("打开获取数据链接...")
  }
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

  override def close(): Unit = {

    println("关闭获取数据链接...")
  }
}
