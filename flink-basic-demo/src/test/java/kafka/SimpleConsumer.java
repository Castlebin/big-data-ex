package kafka;

import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class SimpleConsumer {
    public static void main(String[] args) throws InterruptedException {
        KafkaConsumer<String, String> consumer = buildKafkaConsumer("localhost:9092", "SimpleConsumer_GROUP");
        // 设置要订阅的 topic
        consumer.subscribe(Arrays.asList("simple-topic"));

        while (true) {
            ConsumerRecords<String, String> msgList = consumer.poll(5000);
            System.out.println("get msgList " + new Date());
            if (msgList != null && !msgList.isEmpty()) {
                for (ConsumerRecord<String, String> record : msgList) {
                    System.out.println(new Date() + " : " + record.key() + " : " + record.value());
                }
            }
        }
    }

    private static KafkaConsumer<String, String> buildKafkaConsumer(String broker, String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");    // 是否自动提交
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("max.poll.records", 3);    // 每次最多拉取条数
        props.put("auto.offset.reset", "earliest"); // 设置从哪里开始消费
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

}
