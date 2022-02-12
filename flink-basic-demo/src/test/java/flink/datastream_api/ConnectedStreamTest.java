package flink.datastream_api;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * 这里注意两个流只有键一致的时候才能连接。
 * keyBy 的作用是将流数据分区，当 keyed stream 被连接时，他们必须按相同的方式分区。
 * 这样保证了两个流中所有键相同的事件发到同一个实例上。这样也使按键关联两个流成为可能。
 *
 * ！！ TODO 不太理解本示例结果的输出，而且每次输出结果会不一样？？
 * （猜测是因为两个流，在运行时，分别会以不确定的速度进入flink处理，实际应用中，应该结合时间窗口来使用，避免出现这种情况）
 */
public class ConnectedStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> control = env
                .fromElements("DROP", "IGNORE")
                .keyBy(x -> x);

        DataStream<String> streamOfWords = env
                .fromElements("Apache", "DROP", "Flink", "IGNORE")
                .keyBy(x -> x);

        // Connected Stream ：两个流的连接
        // 在这个例子中，两个流都是 DataStream<String> 类型的，并且都将字符串作为键。
        // 正如你将在下面看到的，RichCoFlatMapFunction 在状态中存了一个布尔类型的变量，这个变量被两个流共享。
        control
                .connect(streamOfWords)
                .flatMap(new ControlFunction())
                .print();

        env.execute();
    }

    /**
     * RichCoFlatMapFunction
     */
    public static class ControlFunction extends RichCoFlatMapFunction<String, String, String> {
        private ValueState<Boolean> blocked;

        @Override
        public void open(Configuration config) {
            blocked = getRuntimeContext()
                    .getState(new ValueStateDescriptor<>("blocked", Boolean.class));
        }

        @Override
        public void flatMap1(String control_value, Collector<String> out) throws Exception {
            blocked.update(Boolean.TRUE);
        }

        @Override
        public void flatMap2(String data_value, Collector<String> out) throws Exception {
            if (blocked.value() == null) {
                out.collect(data_value);
            }
        }
    }
}
