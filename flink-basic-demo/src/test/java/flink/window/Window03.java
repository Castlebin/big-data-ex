package flink.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.junit.jupiter.api.Test;

/**
 * flink 有两种不同的划分窗口的方式：             <br />
 * 1. 根据时间 （TimeWindow）                     <br />
 * 2. 根据个数（CountWindow）                     <br />
 *                                                  <br />
 * 以上两种，又可以分为是 滚动窗口、滑动窗口                <br />
 * 另外，时间窗口还有一种特殊的窗口，被称为 SessionWindow     <br />
 */
public class Window03 {

    /**
     * 1. 以 EventTime 作为 Timestamps 的单词统计，带时间窗口
     * （使用了一些老的 API 方式，新的 API 已经不是这么用了，好像更简洁些）
     */
    @Test
    public void timeWindow01() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1. 设置使用 EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Socket 输入作为数据源，每行两个字段，第一个表示时间戳，第二个是单词，空格分割
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = env.socketTextStream("localhost", 11111)
                // 先设置 watermarks 为 0
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(String line) {
                        // 第一个字段为时间戳
                        return Long.parseLong(line.split(" ")[0]);
                    }
                })
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String line) throws Exception {
                        // 第二个字段为单词
                        return new Tuple2<>(line.split(" ")[1], 1L);
                    }
                })
                // 按 单词 keyBy
                .keyBy(0);

        // 引入 滚动窗口，窗口时长为 10 s
        WindowedStream<Tuple2<String, Long>, Tuple, TimeWindow> windowedStream =
                keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        // 执行聚合操作
        DataStream<Tuple2<String, Long>> wordCountStream = windowedStream
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2)
                            throws Exception {
                        return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
                    }
                });

        wordCountStream.print();

        env.execute("timeWindow01");
    }

}
