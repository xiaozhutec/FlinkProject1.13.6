package com.johngo.streaming.customeSource

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author Johngo
 * @date 2022/4/6
 */

object StreamingWithJGRichParallelSourceScala {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //隐式转换
    import org.apache.flink.api.scala._
    val text = env.addSource(new JGRichParallelSourceScala)

    var recieveData = text.map(line => {
      line
    })

    recieveData.print("接收到的数据：")

    env.execute("StreamingWithJGParalleSourceScala")
  }

}
