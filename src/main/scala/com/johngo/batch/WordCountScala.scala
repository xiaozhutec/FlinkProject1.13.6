package com.johngo.batch

import org.apache.flink.api.scala.ExecutionEnvironment


/**
 * @author Johngo
 * @date 2022/4/1
 */

object WordCountScala {
  def main(args: Array[String]): Unit = {
    val filePath = "./datas/dm.csv"
    val resultPath = "./datas/wc_rst.csv"

    // 获取运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile(filePath)

    //引入隐式转换
    import org.apache.flink.api.scala._
    val counts = text.flatMap { _.toLowerCase.split(",") filter { _.nonEmpty } }
      .map((_, 1))
      .groupBy(0)
      .sum(1)
    counts.print()
    counts.writeAsCsv(resultPath, "\n", " ")
  }
}
