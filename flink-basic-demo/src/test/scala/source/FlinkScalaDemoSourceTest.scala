package source

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object FlinkScalaDemoSourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 坑货啊，IDE 中执行，scala 和 java 对目录路径的认识不一样，scala 是整个大工程作为识别的目录
    val stream = env.readTextFile("hello.txt")

    stream.print()

    env.execute("FirstJob")
  }
}
