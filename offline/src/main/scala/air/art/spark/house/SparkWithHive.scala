package air.art.spark.house

import org.apache.spark.sql.SparkSession

object SparkWithHive {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]")
      .config("spark.num-executors","2")
      .config("spark.executor.cores","2")
      .config("spark.executor.memory","5g")
      .config("spark.sql.warehouse.dir","offline/src/main/resources/warehouse")
      .appName("Spark Option Hive")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("show databases").show()
    //while (true) {}
  }

}
