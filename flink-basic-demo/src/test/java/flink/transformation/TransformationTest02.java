package flink.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

public class TransformationTest02 {

    /**
     * 1. keyBy   对流中的元素进行分组，得到一个 KeyedStream
     */
    @Test
    public void keyBy() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source = env.readTextFile("data/hello-world.txt");

        // 1. 对每行数据进行 单词分割
        DataStream<String> wordStream =
                source.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String line, Collector<String> collector) throws Exception {
                        String[] words = line.split(" ");
                        for (String word : words) {
                            collector.collect(word);
                        }
                    }
                });

        // 2. 将分割好的单词，转化为 (word, 1) 形式的 tuple , 便于后面做单词数目统计
        DataStream<Tuple2<String, Integer>> wordCountStream = wordStream
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String word) throws Exception {
                        return new Tuple2<>(word, 1);
                    }
                });

        // 3. keyBy 操作，将流转化为 KeyedStream ，便于后面对每个单词执行统计   （这里的 0 表示 按 tuple 的 0 号位 字段 keyBy）
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordCountStream.keyBy(0);

        // keyBy  可以看到，打印出来的结果，同样的key，分配到同一个 线程 上进行处理的
        keyedStream.print();

        env.execute("keyBy");
    }


    /**
     * 2. reduce 1
     */
    @Test
    public void reduce01() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source = env.readTextFile("data/hello-world.txt");

        // 上面的代码先照搬下来，直到 执行 reduce
        // 1. 对每行数据进行 单词分割
        DataStream<String> wordStream =
                source.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String line, Collector<String> collector) throws Exception {
                        String[] words = line.split(" ");
                        for (String word : words) {
                            collector.collect(word);
                        }
                    }
                });

        // 2. 将分割好的单词，转化为 (word, 1) 形式的 tuple , 便于后面做单词数目统计
        DataStream<Tuple2<String, Integer>> wordCountStream = wordStream
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String word) throws Exception {
                        return new Tuple2<>(word, 1);
                    }
                });

        // 3. keyBy 操作，将流转化为 KeyedStream ，便于后面对每个单词执行统计   （这里的 0 表示 按 tuple 的 0 号位 字段 keyBy）
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordCountStream.keyBy(0);

        // keyBy  可以看到，打印出来的结果，同样的key，分配到同一个 线程 上进行处理的
    //    keyedStream.print();

        DataStream<Tuple2<String, Integer>> reducedStream = keyedStream
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1,
                            Tuple2<String, Integer> t2) throws Exception {
                        // 单词数目相加
                        return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
                    }
                });
        // 打印看看结果，发现并不是期望中的单词统计，而是把所有计算步骤的结果都打印出来了
        /**
         * 对 KeyedStream 的后续聚合操作，必须加上窗口，否则每一步计算步骤都会产生结果（流式处理，跟 Stream 这种壁批处理不一样，理解一下）
         */
        reducedStream.print();

        env.execute("reduce01");
    }

}
