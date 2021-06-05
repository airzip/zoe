package com.ibm.offline.etl.hive

import org.apache.spark.sql.SparkSession

object SparkWithHive {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local")
      .config("spark.num-executors","1")
      .config("spark.executor.cores","1")
      .config("spark.executor.momory","1g")
      .config("spark.sql.warehouse.dir","src/main/resources/warehouse")
      .appName("Spark Option Hive")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("show databases").show()
  }

}
