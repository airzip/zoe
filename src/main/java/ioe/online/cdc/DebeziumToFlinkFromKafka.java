package ioe.online.cdc;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class DebeziumToFlinkFromKafka {
    public static void main(String[] args) throws Exception {
        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT,"12345");
        //conf.setInteger(TaskManagerOptions.NUM_TASK_SLOTS,8);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .inStreamingMode().useBlinkPlanner().build();
        //TableEnvironment tableEnvironment = TableEnvironment.create(environmentSettings);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, environmentSettings);
        tableEnvironment.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        // 中汇关系任意数据库 CDC捕获时间流进行解析成 Flink SQL 表数据
        tableEnvironment.executeSql("CREATE TABLE customers (\n" +
                "  id INT,\n" +
                " first_name STRING,\n" +
                " last_name STRING,\n" +
                " email STRING \n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'dbserver1.inventory.customers',\n" +
                " 'properties.bootstrap.servers' = '127.0.0.1:9092',\n" +
                " 'debezium-json.schema-include' = 'true',\n" +
                " 'properties.group.id' = 'flink',\n" +
                " 'format' = 'debezium-json'\n" +
                ")"
        );

        // 目标中汇关系任意数据库
        tableEnvironment.executeSql("CREATE TABLE customers_copy (\n" +
                        " id INT,\n" +
                        " first_name VARCHAR(1000),\n" +
                        " last_name VARCHAR(1000),\n" +
                        " email VARCHAR(1000), \n" +
                        " PRIMARY KEY (id) NOT ENFORCED \n" +
                        ") WITH (\n" +
                        " 'connector' = 'jdbc',\n" +
                        // " 'driver' = 'com.mysql.jdbc.Driver',\n" +
                        // " 'url' = '" + "jdbc:mysql://localhost:3306/inventory?useSSL=false" + "',\n" +
                        // "'driver'= '" + "org.postgresql.Driver" + "',\n" +
                        " 'url' = '" + "jdbc:postgresql://localhost:5432/test" + "',\n" +
                        " 'username' = '" + "db2inst1" + "',\n" +
                        " 'password' = '" + "db2inst1-pwd" + "',\n" +
                        " 'table-name' = '" + "CUSTOMERS" + "'\n" +
                        ")"
        );
        //String updateSQL = "select * from customers limit 3";
        String updateSQL = "insert into customers_copy select * from customers";
        TableResult result = tableEnvironment.executeSql(updateSQL);
        // Table result = tableEnvironment.sqlQuery(updateSQL);
        // tableEnvironment.toRetractStream(result, Row.class).print();
        // env.execute("CDC");
    }
}
