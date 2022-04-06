package com.johngo.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * @author Johngo
 * @date 2022/4/2
 */

object WordCountSQLScala {
  def main(args: Array[String]): Unit = {
    // 创建运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    import org.apache.flink.api.scala._
    // 从 nc 接入数据， 数据格式：word,word2,word3
    val dataStream = env.socketTextStream("localhost", 8899, '\n')
      .flatMap(line => line.split(','))
      .map(word => WC(word, 1L))

    // 转换为一个表(table) & 查询
    val inputTable = tableEnv.fromDataStream(dataStream)
    tableEnv.createTemporaryView("WC", inputTable)
    val resultTable = tableEnv.sqlQuery("SELECT word, SUM(`count`) FROM WC GROUP BY word")

    // toAppendStream toRetractStream
    val resValue = tableEnv.toChangelogStream(resultTable)
    resValue.print().setParallelism(1)

    env.execute("WordCountSQLScala")
  }

  case class WC(word: String, count: Long)
}
