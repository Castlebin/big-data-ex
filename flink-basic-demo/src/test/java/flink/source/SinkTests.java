package flink.source;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class SinkTests {

    private static StreamExecutionEnvironment env;

    @BeforeAll
    public static void before() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    /**
     * kafka 作为 Sink
     * <p>
     * 按 SimpleProducer.java 中的操作，启动 kafka 即可做测试
     * <p>
     * * 注意：用 nc 命令，打开一个 Socket 端口
     * * Mac :  nc -l 11111          , 然后，就可以在命令窗口中，输入文字了，相当于向该 socket 端口写入数据
     */
    @Test
    public void testKafkaAsSource() throws Exception {
        // 使用 socket 作为数据源
        DataStreamSource<String> source = env.socketTextStream("localhost", 11111);
        source.print();

        String topic = "simple-topic";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");

        // 使用 kafka 作为 Sink，向 kafka 中写入数据
        source.addSink(new FlinkKafkaProducer<>(topic,
                new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), properties));

        env.execute();
    }

}
