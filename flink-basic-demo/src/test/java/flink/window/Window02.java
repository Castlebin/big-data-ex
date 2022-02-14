package flink.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.junit.jupiter.api.Test;

/**
 * flink 有两种不同的划分窗口的方式：             <br />
 * 1. 根据时间 （TimeWindow）                     <br />
 * 2. 根据个数（CountWindow）                     <br />
 * <br />
 * 以上两种，又可以分为是 滚动窗口、滑动窗口                <br />
 * 另外，时间窗口还有一种特殊的窗口，被称为 SessionWindow     <br />
 */
public class Window02 {

    /**
     * 0. TimeWindow 滚动窗口  (执行失败)
     *  （Caused by: java.lang.RuntimeException:
     *  Record has Long.MIN_VALUE timestamp (= no timestamp marker).
     *  Is the time characteristic set to 'ProcessingTime',
     *  or did you forget to call 'DataStream.assignTimestampsAndWatermarks(...)'?）
     * timeWindow() 这个 API 已经废弃了，应该用正确的结合 时间戳设置 和 watermarks 策略的方式来使用时间窗口！
     */
    @Test
    public void timeWindow00() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Socket 输入作为数据源，方便起见，每行一个单词
        DataStream<String> source = env.socketTextStream("localhost", 11111);

        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = source
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String word) throws Exception {
                        return new Tuple2<>(word, 1L);
                    }
                })
                .keyBy(0);

        // 设定滚动窗口大小  5s (TimeWindow 默认的时间策略是 flink 算子的 处理时间)
        WindowedStream<Tuple2<String, Long>, Tuple, TimeWindow> timeWindowStream =
                keyedStream.timeWindow(Time.seconds(5));
        DataStream<Tuple2<String, Long>> wordCountStream =
                timeWindowStream.reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2)
                            throws Exception {
                        return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
                    }
                });
        // 对 keyedStream 执行 窗口计算
        wordCountStream.print();

        env.execute("timeWindow00");
    }

}
