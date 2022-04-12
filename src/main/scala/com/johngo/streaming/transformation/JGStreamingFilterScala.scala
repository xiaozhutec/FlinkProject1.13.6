package com.johngo.streaming.transformation

import com.johngo.streaming.customeSource.JGNoParallelSourceScala
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * @author Johngo
 * @date 2022/4/11
 */

object JGStreamingFilterScala {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //隐式转换
    import org.apache.flink.api.scala._
    val text = env.addSource(new JGNoParallelSourceScala).setParallelism(1)

    var mapData = text.map(line => {
      println("接收到的原始数据是：" + line);
      line
    })

    // filter 操作
    val filterData: DataStream[Long] = mapData.filter(line => {
      if (line % 2 == 0) {
        println("被保留：" + line)
        return true
      } else {
        println("被舍弃：" + line)
        return false
      }
    })

    filterData.map( line => {
      println("过滤之后的数据是：" + line);
      line
    })

    env.execute("JGStreamingFilterScala")
  }
}
