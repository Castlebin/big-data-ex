package flink.transformation;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

public class TransformationTest02 {

    /**
     * 1. keyBy   对流中的元素进行分组，得到一个 KeyedStream
     */
    @Test
    public void keyBy() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Long> source = env.fromSequence(1, 6);
        KeyedStream<Long, Long> keyedStream = source
                .keyBy(x -> x % 2);

        // keyBy 之后，需要跟其他的处理。这里仅仅只是为了打印，为了让代码能执行而已
        keyedStream.print();


        env.execute("keyBy");
    }

}
