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

package spendreport;


import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

/**
 * Skeleton code for implementing a fraud detector.
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

	private static final long serialVersionUID = 1L;

	private static final double SMALL_AMOUNT = 1.00;
	private static final double LARGE_AMOUNT = 500.00;
	private static final long ONE_MINUTE = 60 * 1000;

	// 保存状态
	private transient ValueState<Boolean> flagState;
	// 保存定时器时间状态
	private transient ValueState<Long> timerState;

	@Override
	public void processElement(
			Transaction transaction,
			Context context,
			Collector<Alert> collector) throws Exception {

		// 1. 获取现在的状态（上次交易状态是否为小额交易）
		Boolean lastTransactionWasSmall = flagState.value();
		if (lastTransactionWasSmall != null) {
			if (transaction.getAmount() > LARGE_AMOUNT) {
				// 2. 上次交易为小额交易，本次为大额交易，生成一条警报
				Alert alert = new Alert();
				alert.setId(transaction.getAccountId());

				collector.collect(alert);
			}

			// 清理掉状态
			cleanUp(context);
		}
		if (transaction.getAmount() < SMALL_AMOUNT) {
			// 设置状态，本次交易为小额交易
			flagState.update(true);

			// 设置定时器和定时器状态变量（注意：这里使用的是 processingTime，往后推迟 1 分钟）
			long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
			context.timerService().registerProcessingTimeTimer(timer);
			timerState.update(timer);
		}
	}

	/**
	 * 当定时器触发时，将会调用 onTimer 方法。 通过重写这个方法来实现一个你自己的重置状态的回调逻辑。
	 */
	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
		// remove flag after 1 minute
		timerState.clear();
		flagState.clear();
	}

	/**
	 * 自定义的清理逻辑：1. 取消（删除）定时器；2. 清理状态变量
	 */
	private void cleanUp(Context ctx) throws Exception {
		// 删除定时器
		Long timer = timerState.value();
		ctx.timerService().deleteProcessingTimeTimer(timer);

		// 清理状态
		timerState.clear();
		flagState.clear();
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
				"flag",
				Types.BOOLEAN);
		flagState = getRuntimeContext().getState(flagDescriptor);

		ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
				"timer-state",
				Types.LONG
		);
		timerState = getRuntimeContext().getState(timerDescriptor);
	}
}
