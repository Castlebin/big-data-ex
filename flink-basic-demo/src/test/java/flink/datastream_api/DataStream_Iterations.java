package flink.datastream_api;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class DataStream_Iterations {

    private static StreamExecutionEnvironment env;

    @BeforeAll
    public static void before() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    /**
     * IterativeStream
     */
    @Test
    public void iterative_example_1() throws Exception {
        // 生成数字序列
        DataStream<Long> someIntegers = env.fromSequence(0, 20);

        IterativeStream<Long> iteration = someIntegers.iterate();

        DataStream<Long> minusOne = iteration.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                return value - 1 ;
            }
        });

        DataStream<Long> stillGreaterThanZero = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return (value > 0);
            }
        });

        iteration.closeWith(stillGreaterThanZero);

        DataStream<Long> lessThanZero = minusOne.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return (value <= 0);
            }
        });

        lessThanZero.print();

        env.execute();
    }

}
