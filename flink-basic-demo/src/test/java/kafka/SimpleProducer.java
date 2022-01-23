package kafka;

import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 kafka 命令，进入 kafka 目录后 (为方便观察，以下命令都使用前台模式执行)：
 1. 启动 zookeeper ： bin/zookeeper-server-start.sh config/zookeeper.properties
 2. 启动 kafka server ： bin/zookeeper-server-start.sh config/zookeeper.properties
 3. （做实验，启动一个kafka生产者，可以往 topic 里发送消息，topic名字：simple-topic）：
    bin/kafka-console-producer.sh --topic simple-topic --bootstrap-server localhost:9092
 4. 启动个 kafka 消费者，监听 topic，topic名字：simple-topic ，消费模式可以设置为从头开始（--from-beginning）
    bin/kafka-console-consumer.sh --topic simple-topic --from-beginning --bootstrap-server localhost:9092
 */
public class SimpleProducer {

    public static void main(String[] args) {
        KafkaProducer<String, String> kafkaProducer = buildKafkaProducer();
        sendMessages(kafkaProducer, "simple-topic");
    }

    private static void sendMessages(KafkaProducer<String, String> kafkaProducer, String topic) {
        for (int index = 0; index < 1000; index++) {
            String message = System.currentTimeMillis() + "," + new Date() + ",hello";
            kafkaProducer.send(new ProducerRecord<>(topic, message));
            System.out.println("send message to kafka : " + message);

            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    // 使用配置，生成一个 kafka 生产者，用它来发送消息到 kafka 服务器
    private static KafkaProducer<String, String> buildKafkaProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

}
