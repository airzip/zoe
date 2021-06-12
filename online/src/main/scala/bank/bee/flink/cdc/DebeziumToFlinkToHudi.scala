package bank.bee.flink.cdc

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object DebeziumToFlinkToHudi {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val set = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()
    val tableEnv = StreamTableEnvironment.create(env,set)
    tableEnv.getConfig.setSqlDialect(SqlDialect.DEFAULT)

    tableEnv.executeSql(
      """
        |"CREATE TABLE customer(\n" +
        |      "id VARCHAR(20),\n" +
        |      "name VARCHAR(10),\n" +
        |      "age int,\n" +
        |      "user_level VARCHAR(20) \n" +
        |      ") WITH (\n" +
        |      "  'connector' = 'kafka',\n" +
        |      "  'topic' = '" + 'topicTest' + "',\n" +
        |      "  'properties.bootstrap.servers' = '" + 'localhost:9092' + "',\n" +
        |      "  'properties.group.id' = '" + 'groupTest' + "',\n" +
        |      "  'debezium-json.schema-include' = 'true',\n" +
        |      "  'format' = 'debezium-json'\n" +
        |      ")"
        |""".stripMargin
    )

    tableEnv.executeSql(
      """
        |"CREATE TABLE customers_hudi(\n" +
        |      "id VARCHAR(20) PRIMARY KEY NOT ENFORCED,\n" +
        |      "name VARCHAR(10),\n" +
        |      "age INT,\n" +
        |      "ts TIMESTAMP(3), \n" +
        |      "`user_level` VARCHAR(20) ) \n" +
        |      "PARTITIONED BY (user_level) \n" +
        |      "WITH (\n" +
        |      "  'connector' = 'hudi',\n" +
        |      "  'path' = '" + table_base_path +"',\n" +
        |      "  'table.type' = '" + table_type + "',\n" +
        |      "  'read.tasks' = '1',\n" +
        |      "  'write.tasks' = '1',\n" +
        |      "  'compaction.tasks' = '1',\n" +
        |      "  'write.batch.size' = '8',\n" +
        |      "  'compaction.delta_commits' = '2',\n" +
        |      "  'compaction.delta_seconds' = '10' " +
        |      ")"
        |""".stripMargin
    )

    tableEnv.executeSql(
      """
        |"INSERT INTO	customers_hudi (\n" +
        |" id, name, age, ts, user_level)\n" +
        |"SELECT \n" +
        |"id,	name,	age,	current_timestamp,	user_level \n" +
        |"FROM	customer \n" +
        |""".stripMargin
    )
  }

}
