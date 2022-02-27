package flink.window;

import java.time.Duration;
import java.util.Objects;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.jupiter.api.Test;

import flink.pojo.LogEvent;

public class Window04 {

    @Test
    public void loginLogoutAnalysis() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> source = env.readTextFile("data/login_logout.csv");

        DataStream<LogEvent> events =
                source.map(new MapFunction<String, LogEvent>() {
                            @Override
                            public LogEvent map(String line) throws Exception {
                                try {
                                    String[] values = line.split(",");
                                    return new LogEvent(Long.parseLong(values[0]),
                                            Long.parseLong(values[1]),
                                            values[2],
                                            values[3]);
                                } catch (Exception e) {
                                    return null;
                                }
                            }
                        })
                        .filter(Objects::nonNull)
                        // 时间戳和 watermarks
                        .assignTimestampsAndWatermarks(
                                // 这里还有个泛型需要写，麻烦!
                                WatermarkStrategy.<LogEvent> forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                        .withTimestampAssigner(
                                                (SerializableTimestampAssigner<LogEvent>) (event, t) -> event.getTime()));

        KeyedStream<LogEvent, Long> keyedStream = events.keyBy(LogEvent::getUserId);

        // 结果并不是每人每天一条，不知道为什么？？？ todo
        keyedStream
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .maxBy("time")
                .print();

        env.execute("loginLogoutAnalysis");
    }

}
