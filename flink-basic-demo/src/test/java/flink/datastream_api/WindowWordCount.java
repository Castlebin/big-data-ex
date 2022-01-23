package flink.datastream_api;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WindowWordCount {

    /**
     * 以 Socket 输入为 Source
     * 所以，提前在命令行中，使用 netcat 打开一个 Socket 写入端口，用于输入数据
     * 命令:
     *   nc -lk 9999   （9999 是端口号）
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);

        dataStream.print();

    //  env.execute("Window WordCount");

        // 换成 异步API
        JobClient jobClient = env.executeAsync("Window WordCount");
        jobClient.getJobExecutionResult().get();
    }

    // 一行可以输入多个单词，也就是一句话，所以用 flatMap
    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }

}