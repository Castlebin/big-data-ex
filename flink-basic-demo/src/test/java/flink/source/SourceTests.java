package flink.source;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
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
     * kafka 作为 Source
     *
     * 按 SimpleProducer.java 中的操作，启动 kafka 即可做测试
     */
    @Test
    public void testKafkaAsSource() throws Exception {
        String topic = "simple-topic";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id","test");

        DataStreamSource<String> source =
                env.addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties));
        source.print();

        env.execute();
    }

    /**
     * 文本单词统计
     * （流式处理，所以可以看到每来一条数据，就打印一次结果
     *  hello-world.txt  共 11 条数据，每行 2 个单词，空格隔开，所以最后转化为 22 条数据
     *  可以看到结果也是打印 22 次，可以每次都会统计下结果
     *  看 hello 就可以知道了
     * ）
     */
    @Test
    public void wordCountFile() throws Exception {
        DataStreamSource<String> source = env.readTextFile("data/hello-world.txt");

        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream = source.flatMap(new Splitter())
                .keyBy(value -> value.f0)
                .sum(1);

        dataStream.print();

        env.execute();
    }

    /**
     * 文本单词统计
     * （用批处理，所有数据只输出一次结果，可以看到是正常的单词统计结果。用流处理会是不一样的结果，想想为什么？）
     */
    @Test
    public void wordCountFile_2() throws Exception {
        StreamExecutionEnvironment env_batch = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置为 批处理模式 (这里也可以用 RuntimeExecutionMode.AUTOMATIC ，可以看到结果是一样的 )
        env_batch.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStreamSource<String> source = env_batch.readTextFile("data/hello-world.txt");

        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream = source.flatMap(new Splitter())
                .keyBy(value -> value.f0)
                .sum(1);

        dataStream.print();

        env_batch.execute();
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
