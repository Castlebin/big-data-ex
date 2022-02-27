package flink.source;

import static org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import flink.pojo.UserReport;

public class SourceTests {

    private static StreamExecutionEnvironment env;

    @BeforeAll
    public static void before() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    /**
     * 1. 使用 文本文件 作为 Source
     */
    @Test
    public void testFileAsSource() throws Exception {
        DataStreamSource<String> source = env.readTextFile("data/hello-world.txt");
        source.print();

        env.execute();
    }

    /**
     * 2. 使用 Socket 作为 Source
     *
     * 注意：先用 nc 命令，打开一个 Socket 端口
     * Mac :  nc -l 11111          , 然后，就可以在命令窗口中，输入文字了，相当于向该 socket 端口写入数据
     */
    @Test
    public void testSocketAsSource() throws Exception {
        DataStreamSource<String> source = env.socketTextStream("localhost", 11111);
        source.print();

        env.execute();
    }

    /**
     * 3. 集合作为 Source
     */
    @Test
    public void testCollectionAsSource() throws Exception {
        List<String> list = new ArrayList<>();
        list.add("1");
        list.add("2");
        list.add("3");

        DataStreamSource<String> source = env.fromCollection(list);
        source.print();

        env.execute();
    }

    /**
     * 4. 生成序列 作为 Source
     * generateSequence 废弃了，用 fromSequence 代替
     */
    @Test
    public void testSequenceAsSource() throws Exception {
        DataStreamSource<Long> source = env.generateSequence(1, 10);
        source.print();

        env.execute();
    }
    @Test
    public void testSequenceAsSource2() throws Exception {
        DataStreamSource<Long> source = env.fromSequence(1, 10);
        source.print();

        env.execute();
    }

    /**
     * kafka 作为 Source
     *
     * 按 SimpleProducer.java 中的操作，启动 kafka 即可做测试
     */
    @Test
    public void testKafkaAsSource() throws Exception {
        String topic = "simple-topic";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("group.id","test");

        DataStreamSource<String> source =
                env.addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties));
        source.print();

        env.execute();
    }

    /**
     * FlinkKafkaConsumer 已经废弃，用 KafkaSource 代替
     * https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/connectors/datastream/kafka/#kafka-source
     */
    @Test
    public void testKafkaSource() throws Exception {
        String topic = "simple-topic";

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics(topic)
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> source =
                env.fromSource(kafkaSource, noWatermarks(), "Kafka Source");
        source.print();

        env.execute();
    }
    // 配置一个解码器，直接把 kafka 消息，解析为一个对象
    @Test
    public void testKafkaSource2() throws Exception {
        String topic = "simple-topic";

        KafkaSource<Person> kafkaSource = KafkaSource.<Person>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics(topic)
                .setGroupId("my-group")
                // 改成从kafka最新位点读
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new MyPersonDeserializer())
                .build();

        DataStreamSource<Person> source =
                env.fromSource(kafkaSource, noWatermarks(), "Kafka Source");
        source.print();

        env.execute();
    }

    /**
     * 自定义 Source （实现 SourceFunction 接口 , 还有个子接口，RichSourceFunction，用它的话可以，可以实现更丰富的功能，例如：状态管理）
     * MyPersonSource 实现了 SourceFunction 接口
     */
    @Test
    public void testMySource() throws Exception {
        DataStreamSource<Person> source = env.addSource(new MyPersonSource());

        source.print();

        env.execute();
    }

    /**
     * 文本单词统计
     * （流式处理，所以可以看到每来一条数据，就打印一次结果
     *  hello-world.txt  共 11 条数据，每行 2 个单词，空格隔开，所以最后转化为 22 条数据
     *  可以看到结果也是打印 22 次，可以看到每次都会统计下结果
     *  看 hello 就可以知道了
     * ）
     */
    @Test
    public void wordCountFile() throws Exception {
        DataStreamSource<String> source = env.readTextFile("data/hello-world.txt");

        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream = source.flatMap(new Splitter())
                .keyBy(value -> value.f0)
                .sum(1);

        dataStream.print();

        env.execute();
    }

    /**
     * 文本单词统计
     * （用批处理，所有数据只输出一次结果，可以看到是正常的单词统计结果。用流处理会是不一样的结果，想想为什么？）
     */
    @Test
    public void wordCountFile_2() throws Exception {
        StreamExecutionEnvironment env_batch = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置为 批处理模式 (这里也可以用 RuntimeExecutionMode.AUTOMATIC ，可以看到结果是一样的 )
        env_batch.setRuntimeMode(RuntimeExecutionMode.BATCH);
        DataStreamSource<String> source = env_batch.readTextFile("data/hello-world.txt");

        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream = source.flatMap(new Splitter())
                .keyBy(value -> value.f0)
                .sum(1);

        dataStream.print();

        env_batch.execute();
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }

    public static class Person implements Serializable {
        private String name;
        private int age;

        public Person() {}

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }

    private static class MyPersonDeserializer implements DeserializationSchema<Person> {
        @Override
        public Person deserialize(byte[] bytes) throws IOException {
            String line = new String(bytes);
            try {
                String[] split = line.split("\\s+");
                return new Person(split[0], Integer.parseInt(split[1]));
            } catch (Exception e) {
                System.out.println("字符串：" + line + "，格式不符合 Person 解析格式");
                return null;
            }
        }

        @Override
        public boolean isEndOfStream(Person person) {
            return false;
        }

        @Override
        public TypeInformation<Person> getProducedType() {
            return TypeInformation.of(Person.class);
        }
    }

    private static class MyPersonSource implements SourceFunction<Person> {
        private volatile boolean running = true;

        private String[] names = new String[] {"张三", "李四", "王五", "赵六"};
        private int[] ages = new int[] {18, 17, 20, 19, 25, 37};
        private Random random = new Random(100);

        @Override
        public void run(SourceContext<Person> sourceContext) throws Exception {
            while (running) {
                String name = names[random.nextInt(names.length)];
                int age = ages[random.nextInt(ages.length)];
                sourceContext.collect(new Person(name, age));

                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            this.running = false;
        }
    }

    @Test
    public void testReportData() throws Exception {
        // data/user_report.csv  直接用读取文件方式，会有编码问题，不可见字符！辣鸡玩意儿！UTF-8-With-BOM 的问题，文件转为 UTF-8 就没事了
        DataStreamSource<String> source = env.socketTextStream("localhost", 11111);

        DataStream<UserReport> dataStream = source.map(new MapFunction<String, UserReport>() {
            @Override
            public UserReport map(String line) throws Exception {
                String[] values = line.split(",");
                try {
                    UserReport report = new UserReport();
                    report.setCreativeId(Long.parseLong(values[0]));
                    report.setUnitId(Long.parseLong(values[1]));
                    report.setAccountId(Long.parseLong(values[2]));
                    report.setPhotoId(Long.parseLong(values[3]));
                    report.setUserId(Long.parseLong(values[4]));
                    report.setCreateTime(Long.parseLong(values[5]));
                    report.setAdminId(Long.parseLong(values[6]));
                    report.setAdminTime(Long.parseLong(values[7]));
                    return report;
                } catch (Exception e) {
                    // e.printStackTrace();
                    System.out.println(line + " 不是有效数据行");
                }
                return null;
            }
        }).filter((FilterFunction<UserReport>) Objects::nonNull);

        dataStream.print();

        env.execute();
    }

}
