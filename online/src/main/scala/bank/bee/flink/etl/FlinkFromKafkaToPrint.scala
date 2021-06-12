package bank.bee.flink.etl

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect}

object FlinkFromKafkaToPrint {

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
        |"SELECT \n" +
        |"id,	name,	age,	current_timestamp,	user_level \n" +
        |"FROM	customer \n" +
        |""".stripMargin
    )
  }
}
