package flink.datastream_api;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStream_Iterations {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 生成数字序列
        DataStreamSource<Long> someIntegers = env.fromSequence(0, 5);
        IterativeStream<Long> iteration = someIntegers.iterate();

        SingleOutputStreamOperator<Long> minusOne = iteration.map((MapFunction<Long, Long>) value -> value - 1);
        SingleOutputStreamOperator<Long> stillGreaterThanZero = minusOne.filter((FilterFunction<Long>) value -> value > 0);

        // closeWith 操作，会处理数据流，并将处理结果反馈给原始流的头部
        iteration.closeWith(stillGreaterThanZero);

        SingleOutputStreamOperator<Long> lessOrEqualsZero = minusOne.filter((FilterFunction<Long>) value -> value <= 0);

        lessOrEqualsZero.print();

        env.execute();
    }
}
