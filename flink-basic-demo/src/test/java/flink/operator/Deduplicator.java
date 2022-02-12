package flink.operator;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * 一个更复杂点版的 flatMap , RichFlatMapFunction ，新增的  1.14.0 版本里还没有 (看原理其实可以自己在现有的API上实现)
 *
 * 定义了一个去重的方法，利用了 flink 的状态管理
 *
 * 清理状态 #
 * 上面例子有一个潜在的问题：当键空间是无界的时候将发生什么？Flink 会对每个使用过的键都存储一个 Boolean
 * 类型的实例。如果是键是有限的集合还好，但在键无限增长的应用中，清除再也不会使用的状态是很必要的。这通过在状态对象上调用 clear() 来实现，如下：
 *
 * keyHasBeenSeen.clear()
 * 对一个给定的键值，你也许想在它一段时间不使用后来做这件事。
 */
public class Deduplicator<Event> extends RichFlatMapFunction<Event, Event> {
    ValueState<Boolean> keyHasBeenSeen;

    @Override
    public void open(Configuration conf) {
        ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("keyHasBeenSeen", Types.BOOLEAN);
        keyHasBeenSeen = getRuntimeContext().getState(desc);
    }

    /**
     * 只取第一个事件，用 keyHasBeenSeen 保持状态
     */
    @Override
    public void flatMap(Event event, Collector<Event> out) throws Exception {
        if (keyHasBeenSeen.value() == null) {
            out.collect(event);
            keyHasBeenSeen.update(true);
        }
    }

}
