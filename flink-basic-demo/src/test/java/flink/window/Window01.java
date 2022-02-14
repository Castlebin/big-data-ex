package flink.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.junit.jupiter.api.Test;

/**
 * flink 有两种不同的划分窗口的方式：             <br />
 * 1. 根据时间 （TimeWindow）                     <br />
 * 2. 根据个数（CountWindow）                     <br />
 *                                                  <br />
 * 以上两种，又可以分为是 滚动窗口、滑动窗口                <br />
 * 另外，时间窗口还有一种特殊的窗口，被称为 SessionWindow     <br />
 */
public class Window01 {

    /**
     * 1. CountWindow 滚动窗口
     */
    @Test
    public void countWindow01() throws Exception {
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

        WindowedStream<Tuple2<String, Long>, Tuple, GlobalWindow> countWindowedStream =
                // 设定滚动窗口大小
                // 注意，这里的 5，表示的是 同一个 key 出现 5 次 ，可以在观察输出时发现，并不是所有单词每输入5个就执行一次窗口计算
                keyedStream.countWindow(5);
        DataStream<Tuple2<String, Long>> wordCountStream =
                countWindowedStream.reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2)
                            throws Exception {
                        return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
                    }
                });
        // 对 keyedStream 执行 count window 窗口计算，同一个 key 达到 count 次数，才会触发窗口计算执行，而不是所有的 key 次数和
        wordCountStream.print();

        env.execute("countWindow01");
    }


    /**
     * 2. CountWindow 滑动窗口   countWindow() 有两个参数，第一个表示 窗口大小，第二个表示滑动步长 （每达到滑动步长，就会触发一次执行）
     */
    @Test
    public void countWindow02() throws Exception {
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

        WindowedStream<Tuple2<String, Long>, Tuple, GlobalWindow> countWindowedStream =
                // 设定滚动窗口大小
                // 注意，这里的 5，表示的是 同一个 key 出现 5 次 ，可以在观察输出时发现，并不是所有单词每输入5个就执行一次窗口计算
                // 设置 滑动步长为 2 ，发现，同一个 key，没出现 2 次，就会触发一次窗口计算，计算窗口大小为 5
                keyedStream.countWindow(5, 2);
        DataStream<Tuple2<String, Long>> wordCountStream =
                countWindowedStream.reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2)
                            throws Exception {
                        return new Tuple2<>(t1.f0, t1.f1 + t2.f1);
                    }
                });
        // 对 keyedStream 执行 count window 窗口计算，同一个 key 达到 count 次数，才会触发窗口计算执行，而不是所有的 key 次数和
        wordCountStream.print();

        env.execute("countWindow01");
    }

}
