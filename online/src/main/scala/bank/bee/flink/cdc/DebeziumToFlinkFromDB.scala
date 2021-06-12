package bank.bee.flink.cdc

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

object DebeziumToFlinkFromDB {

  def main(args: Array[String]): Unit = {

    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    //val env = StreamExecutionEnvironment.getExecutionEnvironment
    val set = EnvironmentSettings.newInstance().useBlinkPlanner().build()
    val tableEnv = StreamTableEnvironment.create(env,set)

    tableEnv.sqlQuery(
      """
        |"CREATE TABLE customers (\n" +
        |      "id INT,\n" +
        |      "first_name STRING,\n" +
        |      "last_name STRING,\n" +
        |      "email STRING )\n" +
        |"WITH (\n" +
        |      " 'connector' = 'kafka',\n" +
        |      " 'topic' = 'dbserver1.inventory.customers',\n" +
        |      " 'properties.bootstrap.servers' = '127.0.0.1:9092',\n" +
        |      " 'debezium-json.schema-include' = 'true',\n" +
        |      " 'properties.group.id' = 'flink',\n" +
        |      " 'format' = 'debezium-json'\n" +
        |      ")"
        |""".stripMargin
    )

    tableEnv.sqlQuery(
      """
        |"CREATE TABLE customers_copy (\n" +
        |      " id INT,\n" +
        |      " first_name VARCHAR(1000),\n" +
        |      " last_name VARCHAR(1000),\n" +
        |      " email VARCHAR(1000), \n" +
        |      " PRIMARY KEY (id) NOT ENFORCED) \n" +
        |" WITH (\n" +
        |      " 'connector' = 'jdbc',\n" +
        |       "'driver'= '" + "com.ibm.db2.jcc.DB2Driver" + "',\n" +
        |       " 'url' = '" + "jdbc:db2://localhost:50000/test" + "',\n" +
        |       " 'username' = '" + "db2inst1" + "',\n" +
        |       " 'password' = '" + "db2inst1-pwd" + "',\n" +
        |       " 'table-name' = '" + "CUSTOMERS" + "'\n" +
        |       ")"
        |""".stripMargin
    )
    // " 'driver' = 'com.mysql.jdbc.Driver',\n" +
    // " 'url' = '" + "jdbc:mysql://localhost:3306/inventory?useSSL=false" + "',\n" +
    // "'driver'= '" + "org.postgresql.Driver" + "',\n" +

    tableEnv.executeSql( "INSERT INTO	customers_copy SELECT * FROM	customers" )
  }

}
