package operator;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.training.exercises.EnrichedRide;
import org.apache.flink.training.exercises.EnrichedRide2;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.exercises.common.utils.GeoUtils;
import org.apache.flink.training.solutions.ridecleansing.RideCleansingSolution;
import org.apache.flink.training.solutions.ridecleansing.RideCleansingSolution.NYCFilter;
import org.apache.flink.util.Collector;

public class OperatorTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<TaxiRide> source = env.addSource(new TaxiRideGenerator());

        // 测试一些算子的使用方法，比如说 filter、 map、flatMap 这些的用法

        // 1. filter 、 map
        SingleOutputStreamOperator<EnrichedRide> enrichedRideSource = source.filter(new NYCFilter())
                // map 操作，做转换
                .map(new EnrichedRideMapFunction());

        // 2. flatMap
        SingleOutputStreamOperator<EnrichedRide> flatMapedSource =
                source.flatMap(new NYCEnrichment());


        // 3. keyBy
        // keyBy 对流中元素进行分组
        // （每个 keyBy 会通过 shuffle 来为数据流进行重新分区。总体来说这个开销是很大的，它涉及网络通信、序列化和反序列化。）
        enrichedRideSource
                // 按起始的地点所属的网格分组
                .keyBy(enrichedRide -> enrichedRide.startCell);

        // key 也可以是在需要时，用过计算得到的，而不是固定值或者固定的字段
        source
                // 在需要时计算key
                .keyBy(taxiRide -> GeoUtils.mapToGridCell(taxiRide.startLon, taxiRide.startLat));

        // 为每个行程结束事件创建了一个新的包含 startCell 和时长（分钟）的元组流：
        List<EnrichedRide2> enrichedRide2List = new ArrayList<>();
        DataStreamSource<EnrichedRide2> enrichedRide2Source = env.fromCollection(enrichedRide2List);

        // 4. flatMap
        // 写成 lambda 方式会更简洁方便
        enrichedRide2Source
                .flatMap((FlatMapFunction<EnrichedRide2, Tuple2<Integer, Long>>) (ride, out) -> {
                    // 1. 行程结束的事件
                    if (!ride.isStart) {
                        long duration = ride.endTime - ride.startTime;
                        out.collect(new Tuple2<>(ride.startCell, duration));
                    }
                });

        /** 注意：
         * 尽管状态的处理是透明的，Flink 必须跟踪每个不同的键的最大时长。
         *
         * 只要应用中有状态，你就应该考虑状态的大小。如果键值的数量是无限的，那 Flink 的状态需要的空间也同样是无限的。
         *
         * 在流处理场景中，考虑有限窗口的聚合往往比整个流聚合更有意义。
         */
    }


    /**
     * Map  （只能是 1 -> 1 的映射）
     */
    public static class EnrichedRideMapFunction implements MapFunction<TaxiRide, EnrichedRide> {
        @Override
        public EnrichedRide map(TaxiRide taxiRide) throws Exception {
            return new EnrichedRide(taxiRide);
        }
    }

    /**
     * FlatMap  （1 -> n 的映射，NOTE 这里的 n 可以是 多个，可以是 1 个，也可以是 0 ）
     * 使用接口中提供的 Collector ，flatmap() 可以输出你想要的任意数量的元素，也可以一个都不发。
     */
    public static class NYCEnrichment implements FlatMapFunction<TaxiRide, EnrichedRide> {
        @Override
        public void flatMap(TaxiRide taxiRide, Collector<EnrichedRide> out) throws Exception {
            FilterFunction<TaxiRide> valid = new RideCleansingSolution.NYCFilter();
            // 看到了吧，映射结果 1 或者 是 0 ，任意个
            if (valid.filter(taxiRide)) {
                out.collect(new EnrichedRide(taxiRide));
            }
        }
    }

}
