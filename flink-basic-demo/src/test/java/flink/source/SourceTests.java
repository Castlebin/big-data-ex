package flink.source;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class SourceTests {

    private static StreamExecutionEnvironment env;

    @BeforeAll
    public static void before() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    /**
     * 1. 使用 文本文件 作为 Source
     */
    @Test
    public void testFileAsSource() throws Exception {
        DataStreamSource<String> source = env.readTextFile("data/hello-world.txt");
        source.print();

        env.execute();
    }

    /**
     * 2. 使用 Socket 作为 Source
     *
     * 注意：先用 nc 命令，打开一个 Socket 端口
     * Mac :  nc -l 11111          , 然后，就可以在命令窗口中，输入文字了，相当于向该 socket 端口写入数据
     */
    @Test
    public void testSocketAsSource() throws Exception {
        DataStreamSource<String> source = env.socketTextStream("localhost", 11111);
        source.print();

        env.execute();
    }

    /**
     * 3. 集合作为 Source
     */
    @Test
    public void testCollectionAsSource() throws Exception {
        List<String> list = new ArrayList<>();
        list.add("1");
        list.add("2");
        list.add("3");

        DataStreamSource<String> source = env.fromCollection(list);
        source.print();

        env.execute();
    }

    /**
     * 4. 生成序列 作为 Source
     * generateSequence 废弃了，用 fromSequence 代替
     */
    @Test
    public void testSequenceAsSource() throws Exception {
        DataStreamSource<Long> source = env.generateSequence(1, 10);
        source.print();

        env.execute();
    }
    @Test
    public void testSequenceAsSource2() throws Exception {
        DataStreamSource<Long> source = env.fromSequence(1, 10);
        source.print();

        env.execute();
    }

    /**
     * 结果并不是预期的，没事，简单体验而已
     */
    @Test
    public void wordCountFile() throws Exception {
        DataStreamSource<String> source = env.readTextFile("data/The_Little_Prince.txt");

        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream = source.flatMap(new Splitter())
                .keyBy(value -> value.f0)
                .sum(1);

        dataStream.print();

        env.execute();
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }


}
