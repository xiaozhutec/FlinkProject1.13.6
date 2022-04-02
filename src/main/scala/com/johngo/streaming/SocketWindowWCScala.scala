package com.johngo.streaming

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 互动窗口计算
 * 每隔 1s 计算 2s 内的数据
 *
 * @author Johngo
 * @date 2022/4/1
 */

object SocketWindowWCScala {
  def main(args: Array[String]): Unit = {
    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val hostname = "localhost"
    val port = 8899
    val delimiter = '\n'
    val text = env.socketTextStream(hostname, port, delimiter)

    import org.apache.flink.api.scala._
    // 数据格式：word,word2,word3
    val res = text.flatMap(line => line.split(',')) // 将每一行按照逗号打平
      .map(word => WC(word, 1))
      .keyBy(x => x.word)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(1), Time.seconds(2)))
      .reduce((v1, v2) => WC(v1.word, v1.count + v2.count))

    res.print("data: ").setParallelism(1)

    env.execute("SocketWindowWCScala")
  }

  case class WC(word: String, count: Long)
}
