package flink.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
        DataStream<Tuple2<String, Integer>> wordCountStream = wordStream
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String word) throws Exception {
                        return new Tuple2<>(word, 1);
                    }
                });
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordCountStream.keyBy(0);

        // keyBy  可以看到，打印出来的结果，同样的key，分配到同一个 taskSlot 上进行处理的
        keyedStream.print();

        env.execute("keyBy");
    }

}
