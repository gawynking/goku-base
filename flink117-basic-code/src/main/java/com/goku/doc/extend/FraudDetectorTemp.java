package com.goku.doc.extend;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

/**
 * 告警条件：一笔小额交易后紧随一笔大额交易，检测到这种信息需要报警
 */
public class FraudDetectorTemp extends KeyedProcessFunction<Long, Transaction, Alert> {

    private static final long serialVersionUID = 1L;

    private transient ValueState<Boolean> flagState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
                "flag",
                Types.BOOLEAN);
        flagState = getRuntimeContext().getState(flagDescriptor);
    }

    @Override
    public void processElement(
            Transaction transaction,
            Context context,
            Collector<Alert> collector
    ) throws Exception {

        // Get the current state for the current key
        Boolean lastTransactionWasSmall = flagState.value();

        /**
         * 如果状态不是null，说明上一个是小额交易，需要跟进进一步进行检测
         */
        if (lastTransactionWasSmall != null) {
            if (transaction.getAmount() > 100.0) {
                // Output an alert downstream
                Alert alert = new Alert();
                alert.setId(transaction.getAccountId());

                collector.collect(alert);
            }

            // Clean up our state
            flagState.clear();
        }

        /**
         * 如果遇到小额交易 更新状态值
         */
        if (transaction.getAmount() < 1.0) {
            // Set the flag to true
            flagState.update(true);
        }
    }
}