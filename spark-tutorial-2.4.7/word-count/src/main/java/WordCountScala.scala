import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark  单词统计    scala
 */
object WordCountScala {
  def main(args: Array[String]): Unit = {
    // 创建 Spark 的配置
    val conf = new SparkConf()
    conf.setAppName("WordCountScala")
    conf.setMaster("local")

    // 创建 SparkContext
    val sc = new SparkContext(conf)

    // 1. 加载文件
    val rdd1 = sc.textFile("hello.txt")
    // 2. 拆分单词
    val rdd2 = rdd1.flatMap(_.split(" "))
    // 3. 初始化每个单词数量
    val rdd3 = rdd2.map((_, 1))
    // 4. 聚合（单词计数）
    val rdd4 = rdd3.reduceByKey(_ + _);

    var arr = rdd4.collect();
    arr.foreach(println(_))
  }
}
