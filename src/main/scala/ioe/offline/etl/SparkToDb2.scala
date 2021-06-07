package ioe.offline.etl

import org.apache.spark.sql.SparkSession

object SparkToDb2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local")
      .config("spark.num-executors","1")
      .config("spark.executor.cores","1")
      .config("spark.executor.momory","1g")
      .config("spark.sql.warehouse.dir","src/main/resources/warehouse")
      .appName("IBM DB2")
      .getOrCreate()

    val df = spark.read.format("jdbc")
      .option("driver","com.ibm.db2.jcc.DB2Driver")
      .option("url","jdbc:db2://localhost:50000/sample")
      .option("dbtable","act")
      .option("user","db2inst1")
      .option("password","db2inst1-pwd")
      .load()

    df.printSchema()
    df.createTempView("act")
    spark.sql("SELECT count(1) FROM act").show()

    while (true) {}

  }

}
