package com.heller.wc

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {
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

    // 3. 初始化每个单词数量
    val wordCount = words.map(
      word => (word, 1)
    )
    // 4. 聚合（单词计数）
    val wordToCount = wordCount.reduceByKey(_ + _);

    // 5. 采集结果并打印
    val arr = wordToCount.collect()
    arr.foreach(println(_))


    // 关闭 SparkContext （程序执行完其实会自动关闭的）
    sc.stop()
  }
}
