package org.apache.flink.playgrounds.spendreport;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

public class SpendReportUDF {

    public static Table report(Table transactions) {
        // 用 Table API（SQL） 方式实现逻辑 ，使用自定义函数 MyFloor 来将时间转化为小时级别
        return transactions.select(
                    $("account_id"),
                 // $("transaction_time").floor(TimeIntervalUnit.HOUR).as("log_ts"),
                    call(MyFloor.class, $("transaction_time")).as("log_ts"),
                    $("amount"))
                .groupBy($("account_id"), $("log_ts"))
                .select(
                        $("account_id"),
                        $("log_ts"),
                        $("amount").sum().as("amount"));
    }

    public static class MyFloor extends ScalarFunction {

        @DataTypeHint("TIMESTAMP(3)")
        public LocalDateTime eval(@DataTypeHint("TIMESTAMP(3)") LocalDateTime timestamp) {
            return timestamp.truncatedTo(ChronoUnit.HOURS);
        }

    }

    public static void main(String[] args) throws Exception {
        // flink Table API 使用 TableEnvironment 作为执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // 启动时建立好两张表，一张作为数据源、一张作为结果表
        // 使用 kafka 作为数据源，在 flink 中建立一张交易表 transactions
        tEnv.executeSql("CREATE TABLE transactions (\n" +
                "    account_id  BIGINT,\n" +
                "    amount      BIGINT,\n" +
                "    transaction_time TIMESTAMP(3),\n" +
                "    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic'     = 'transactions',\n" +
                "    'properties.bootstrap.servers' = 'kafka:9092',\n" +
                "    'format'    = 'csv'\n" +
                ")");

        // 一张统计表，作为输出，输出到 MySQL
        tEnv.executeSql("CREATE TABLE spend_report (\n" +
                "    account_id BIGINT,\n" +
                "    log_ts     TIMESTAMP(3),\n" +
                "    amount     BIGINT\n," +
                "    PRIMARY KEY (account_id, log_ts) NOT ENFORCED" +
                ") WITH (\n" +
                "  'connector'  = 'jdbc',\n" +
                "  'url'        = 'jdbc:mysql://mysql:3306/sql-demo',\n" +
                "  'table-name' = 'spend_report',\n" +
                "  'driver'     = 'com.mysql.jdbc.Driver',\n" +
                "  'username'   = 'sql-demo',\n" +
                "  'password'   = 'demo-sql'\n" +
                ")");

        Table transactions = tEnv.from("transactions");

        // 将结果插入 MySQL 表中
        report(transactions)
                .executeInsert("spend_report");  // 结果插入 MySQL
    }

}
