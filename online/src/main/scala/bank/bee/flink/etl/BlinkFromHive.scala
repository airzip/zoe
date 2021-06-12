package bank.bee.flink.etl

import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect, TableEnvironment}
import org.apache.flink.table.catalog.hive.HiveCatalog

object BlinkFromHive {

  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance.useBlinkPlanner.build
    val tableEnv = TableEnvironment.create(settings)

    val hiveCatalog = new HiveCatalog("hive",
      "default", "online/src/main/resources")
    tableEnv.registerCatalog("hive", hiveCatalog)
    tableEnv.useCatalog("hive")
    tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)

    tableEnv.executeSql("show databases")
  }

}
