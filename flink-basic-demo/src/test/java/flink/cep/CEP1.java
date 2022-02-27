package flink.cep;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import flink.pojo.LogEvent;

// Flink CEP doc ： https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/libs/cep/
public class CEP1 {

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

        // CEP 事件处理
        // 1. 首先，定义模式 （begin 登录事件 followedBy 一条登出事件）
        Pattern<LogEvent, LogEvent> pattern = Pattern.<LogEvent> begin("login").where(
                new SimpleCondition<LogEvent>() {
                    @Override
                    public boolean filter(LogEvent event) {
                        return "login".endsWith(event.getType());
                    }
                }
        ).followedBy("logout").where(
                new SimpleCondition<LogEvent>() {
                    @Override
                    public boolean filter(LogEvent event) {
                        return "logout".equals(event.getType());
                    }
                }
        );

        // 2. 进行模式匹配
        PatternStream<LogEvent> patternStream = CEP.pattern(keyedStream, pattern);

        // 3. 对匹配的数据流进行处理
        DataStream<String> result = patternStream.process(
                new PatternProcessFunction<LogEvent, String>() {
                    @Override
                    public void processMatch(Map<String, List<LogEvent>> map, Context context,
                            Collector<String> collector) throws Exception {
                        LogEvent login = map.get("login").get(0);
                        LogEvent logout = map.get("logout").get(0);
                        long userId = login.getUserId();
                        String log = "用户：" + userId + "，登入时间：" + login.getTimeStr()
                                + "，登出时间：" + logout.getTimeStr()
                                + "，时长：" + ((logout.getTime() - login.getTime()) / (1000 * 60)) + " min";
                        collector.collect(log);
                    }
                });
        result.print();

        env.execute("loginLogoutAnalysis");
    }

}
