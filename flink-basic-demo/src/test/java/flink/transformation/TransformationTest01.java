package flink.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

public class TransformationTest01 {

    /**
     * 1. map 操作    1 -> 1 映射
     */
    @Test
    public void map() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> source = env.fromSequence(1, 10);
        source
                .map(x -> x * x)
                .print();

        env.execute("map");
    }

    /**
     * 2. flatMap 操作    1 -> n 映射 , n 可以是多个，也可以是 1 个，还可以是 0 ，不固定
     */
    @Test
    public void flatMap() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> source = env.fromSequence(1, 10);
        source            // 一键转为 lambda 表达式居然报错，类型推断失败，差劲👎🏻
                .flatMap(new FlatMapFunction<Long, String>() {
                    @Override
                    public void flatMap(Long aLong, Collector<String> collector) throws Exception {
                        // 1 -> 1
                        if (aLong % 3 == 0) {
                            collector.collect(aLong + " -> " + aLong * aLong);
                            return;
                        }
                        // 1 -> 2
                        if (aLong % 5 == 0) {
                            collector.collect(aLong + " -> " + aLong);
                            collector.collect(aLong + " -> " + (-aLong));
                        }
                        // 其他 1 -> 0
                    }
                })
                .print();

        env.execute("flatMap");
    }

    /**
     * 3. filter 操作    过滤
     */
    @Test
    public void filter() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> source = env.fromSequence(1, 10);
        source
                .filter(x -> x % 2 == 0)
                .print();

        env.execute("filter");
    }

    /**
     * 4. connect 操作 ，将两个流放到一起，返回一个 ConnectedStream，但两个流其实依然独立，
     * 如果后续还要对 ConnectedStream 进行 map、flatMap、filter 等操作，必须传入两个对应的处理方法，分别对连接前的流1、流2 进行处理
     * 也就是说，连接后的流，内部其实依然保持独立！
     */
    @Test
    public void connect() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> stream01 = env.fromSequence(1, 10);
        DataStreamSource<String> stream02 = env.readTextFile("data/hello-world.txt");

        ConnectedStreams<Long, String> connected = stream01
                .connect(stream02);

        connected.map(new CoMapFunction<Long, String, Object>() {

            // 这里实现对连接前的 流1 的操作
            @Override
            public Object map1(Long aLong) throws Exception {
                return aLong * 2;
            }

            // 这里实现对连接前的 流2 的操作
            @Override
            public Object map2(String s) throws Exception {
                return ":2:" + s;
            }
        })
                .print()
        ;


        env.execute("connect");
    }

}
