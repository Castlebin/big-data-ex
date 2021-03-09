package com.heller.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    // 创建 Spark 配置
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")

    // 创建 SparkContext
    val sc = new SparkContext(conf)

    // 1. 读取文件，获取一行一行的数据
    val lines = sc.textFile("data")
    // 2. 将一行一行的数据进行拆分为一个一个的单词
    val words = lines.flatMap(_.split(" "))
    // 3. 将单词进行分组，便于统计
    val wordGroup = words.groupBy(word => word)
    // 4. 分组计数
    val wordToCount = wordGroup.map{
      case (word, list) => {
        (word, list.size)
      }
    }
    // 5. 采集结果并打印
    val arr = wordToCount.collect()
    arr.foreach(println(_))

    // 关闭 SparkContext （程序执行完其实会自动关闭的）
    sc.stop()
  }
}
