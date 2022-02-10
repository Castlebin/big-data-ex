package flink.datastream_api;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ExampleStreamedPojo {

    /**
     * 测试 Flink 可以对 普通的 POJO 进行序列化
     * （注意 Person 并没有实现 Serializable 接口 ）
     */
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Person> flintstones = env.fromElements(
                new Person("Fred", 35),
                new Person("Wilma", 35),
                new Person("Pebbles", 2));

        // filter 算子，过滤出 age >= 18 的 Person 对象
        DataStream<Person> adults = flintstones
                .filter((FilterFunction<Person>) person -> person.age >= 18);

        adults.print();

        env.execute();
    }

    public static class Person {
        public String name;
        public Integer age;

        public Person() {
        }

        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return this.name + ": age " + this.age.toString();
        }
    }

}
