package flink.window;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * 输出传感器每分钟的温度最大值
 *
 *
 * todo 暂时都没触发计算，why？？
 */
public class SensorMaxValueTest {

    private static StreamExecutionEnvironment env;

    @BeforeAll
    public static void before() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    /**
     * ProcessWindowFunction 窗口全量计算示例
     */
    @Test
    public void testProcessWindowFunction() throws Exception {
        String topic = "simple-topic";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id","test");

        DataStreamSource<String> input =
                env.addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties));
        input.print();

        input.map(new SensorReadingParser())  // 转换文本数据流为 SensorReading 对象流
                .keyBy(SensorReading::getKey)
                .window(TumblingEventTimeWindows.of(Time.minutes(1))) // 滚动窗口
                .process(new MyWastefulMax())
                .print();


        env.execute();
    }

    /**
     * reduce 增量聚合示例
     */
    @Test
    public void testReduceFunction() throws Exception {
        String topic = "simple-topic";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id","test");

        DataStreamSource<String> input =
                env.addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties));
        input.print();
        input.map(new SensorReadingParser()) // 转换文本数据流为 SensorReading 对象流
                .keyBy(SensorReading::getKey)
                .window(TumblingEventTimeWindows.of(Time.minutes(1))) // 滚动窗口
                .reduce(new MyReducingMax(), new MyWindowFunction())
                .print();


        env.execute();
    }

    /**
     * 晚到的事件 #
     * 默认场景下，超过最大无序边界的事件会被删除，但是 Flink 给了我们两个选择去控制这些事件。
     *
     * 您可以使用一种称为旁路输出 的机制来安排将要删除的事件收集到侧输出流中，这里是一个示例:
     *
     * 旁路输出 + 允许延迟
     */
    @Test
    public void testSideOutput() throws Exception {
        String topic = "simple-topic";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id","test");

        DataStreamSource<String> input =
                env.addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties));
        input.print();

        // 定义旁路输出
        OutputTag<SensorReading> lateTag = new OutputTag<SensorReading>("late"){};

        SingleOutputStreamOperator<Tuple3<String, Long, SensorReading>> result =
                input.map(new SensorReadingParser()) // 转换文本数据流为 SensorReading 对象流
                        .keyBy(SensorReading::getKey)
                        .window(TumblingEventTimeWindows.of(Time.minutes(1))) // 滚动窗口
                        .allowedLateness(Time.seconds(10))  // 允许的延迟
                        .sideOutputLateData(lateTag)  // 旁路输出
                        .reduce(new MyReducingMax(), new MyWindowFunction());

        // 获取旁路输出流
        DataStream<SensorReading> lateStream = result.getSideOutput(lateTag);


        env.execute();
    }

    public static class MyWastefulMax extends ProcessWindowFunction<
            SensorReading,                  // 输入类型
            Tuple3<String, Long, Integer>,  // 输出类型
            String,                         // 键类型
            TimeWindow> {                   // 窗口类型

        @Override
        public void process(
                String key,
                Context context,
                Iterable<SensorReading> events,
                Collector<Tuple3<String, Long, Integer>> out) {

            int max = 0;
            for (SensorReading event : events) {
                max = Math.max(event.getValue(), max);
                System.out.println("当前最大：" + max);
            }
            out.collect(Tuple3.of(key, context.window().getEnd(), max));
        }
    }


    private static class MyReducingMax implements ReduceFunction<SensorReading> {
        public SensorReading reduce(SensorReading r1, SensorReading r2) {
            return r1.getValue() > r2.getValue() ? r1 : r2;
        }
    }

    private static class MyWindowFunction extends ProcessWindowFunction<
            SensorReading, Tuple3<String, Long, SensorReading>, String, TimeWindow> {

        @Override
        public void process(
                String key,
                Context context,
                Iterable<SensorReading> maxReading,
                Collector<Tuple3<String, Long, SensorReading>> out) {

            SensorReading max = maxReading.iterator().next();
            out.collect(Tuple3.of(key, context.window().getEnd(), max));
        }
    }


    // 定义一个将数据行，解析为 SensorReading 对象的转换器，数据行包含三列数据，用 , 分隔
    public static class SensorReadingParser implements MapFunction<String, SensorReading> {

        @Override
        public SensorReading map(String data) throws Exception {
            String[] split = data.split(",");
            try {
                SensorReading sensorReading = new SensorReading();
                sensorReading.setKey(split[0]);
                sensorReading.setValue(Integer.valueOf(split[1]));
                sensorReading.setTimestamp(Long.valueOf(split[2]));

                return sensorReading;
            } catch (Exception e) {
                System.out.println("数据格式不正确，data=" + data);
            }

            return null;
        }

    }

}
