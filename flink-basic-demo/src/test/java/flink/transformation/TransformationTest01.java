package flink.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

}
