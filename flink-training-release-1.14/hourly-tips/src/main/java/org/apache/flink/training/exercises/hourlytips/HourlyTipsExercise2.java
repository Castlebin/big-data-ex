/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * 计算出每小时获得小费最多的司机和小费
 *
 * The Hourly Tips exercise from the Flink training.
 *
 * <p>The task of the exercise is to first calculate the total tips collected by each driver, hour
 * by hour, and then from that stream, find the highest tip total in each hour.
 */
public class HourlyTipsExercise2 {

    private final SourceFunction<TaxiFare> source;
    private final SinkFunction<Tuple3<Long, Long, Float>> sink;

    // 定义旁路输出
    private static final OutputTag<TaxiFare> lateFares = new OutputTag<TaxiFare>("lateFares") {};

    /**
     * Creates a job using the source and sink provided.
     */
    public HourlyTipsExercise2(
            SourceFunction<TaxiFare> source, SinkFunction<Tuple3<Long, Long, Float>> sink) {

        this.source = source;
        this.sink = sink;
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        HourlyTipsExercise2 job =
                new HourlyTipsExercise2(new TaxiFareGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    /**
     * Create and execute the hourly tips pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start the data generator
        DataStream<TaxiFare> fares = env.addSource(source)
                // 设置 watermarks
                .assignTimestampsAndWatermarks(
                        // taxi fares are in order  （这里假设事件完全是时间顺序的）
                        WatermarkStrategy.<TaxiFare> forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        (fare, t) -> fare.getEventTimeMillis()));

        // 计算每个司机每小时的小费总和
        SingleOutputStreamOperator<Tuple3<Long, Long, Float>> hourlyTips = fares
                .keyBy((TaxiFare fare) -> fare.driverId)
                .process(new PseudoWindow(Time.hours(1)));

        // find the driver with the highest sum of tips for each hour
        // 返回的 Tuple3 中第二个字段代表 tips ，所以用第二个字段做 max 计算
        DataStream<Tuple3<Long, Long, Float>> hourlyMax =
                // TODO windowAll() 什么意思？不太理解啊
                // 回答：windowAll() 是把所有元素都在一个窗口内计算（和 keyBy 的 window 做对比理解，keyBy 是会将元素按 key 分组，分成各自的窗口
                // 所以，window 的并行度可以是多个，而 windowAll 的并行度是 1）
                // 在 keyby 后数据分流，window 是把不同的key分开聚合成窗口，而 windowall 则把所有的key都聚合起来
                // 所以 windowall的并行度只能为1，而window可以有多个并行度。
                hourlyTips.windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                        .maxBy(2);
        hourlyMax.addSink(sink);

        // 旁路输出
        hourlyTips.getSideOutput(lateFares).print();

        return env.execute("Hourly Tips");
    }


    // 在时长跨度为一小时的窗口中计算每个司机的小费总和。
    // 司机ID作为 key。
    public static class PseudoWindow extends
            KeyedProcessFunction<Long, TaxiFare, Tuple3<Long, Long, Float>> {

        private final long durationMsec;

        // 每个窗口都持有托管的 Keyed state 的入口，并且根据窗口的结束时间执行 keyed 策略。
        // 每个司机都有一个单独的MapState对象。
        private transient MapState<Long, Float> sumOfTips;

        public PseudoWindow(Time duration) {
            this.durationMsec = duration.toMilliseconds();
        }

        // 在初始化期间调用一次。
        @Override
        public void open(Configuration conf) {
            sumOfTips = getRuntimeContext()
                    .getMapState(new MapStateDescriptor<>("sumOfTips", Long.class, Float.class));
        }

        // 每个票价事件（TaxiFare-Event）输入（到达）时调用，以处理输入的票价事件。
        @Override
        public void processElement(
                TaxiFare fare,
                Context ctx,
                Collector<Tuple3<Long, Long, Float>> out) throws Exception {
            long eventTime = fare.startTime.toEpochMilli();
            TimerService timerService = ctx.timerService();

            if (eventTime <= timerService.currentWatermark()) {
                // 事件延迟；其对应的窗口已经触发。放到旁路输出里
                ctx.output(lateFares, fare);
            } else {
                // 将 eventTime 向上取值并将结果赋值到包含当前事件的窗口的末尾时间点。
                long endOfWindow = (eventTime - (eventTime % durationMsec) + durationMsec - 1);

                // 在窗口完成时将启用回调
                timerService.registerEventTimeTimer(endOfWindow);

                // 将此票价的小费添加到该窗口的总计中。
                Float sum = sumOfTips.get(endOfWindow);
                if (sum == null) {
                    sum = 0.0F;
                }
                sum += fare.tip;
                sumOfTips.put(endOfWindow, sum);
            }
        }

        // 当当前水印（watermark）表明窗口现在需要完成的时候调用。
        @Override
        public void onTimer(long timestamp,
                OnTimerContext context,
                Collector<Tuple3<Long, Long, Float>> out) throws Exception {
            long driverId = context.getCurrentKey();
            // 查找刚结束的一小时结果。
            Float sumOfTips = this.sumOfTips.get(timestamp);

            Tuple3<Long, Long, Float> result = Tuple3.of(driverId, timestamp, sumOfTips);
            out.collect(result);
            this.sumOfTips.remove(timestamp);
        }
    }


}
