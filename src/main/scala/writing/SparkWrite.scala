package writing

import org.apache.spark.sql.SparkSession

object SparkWrite {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("spark-csv-write").getOrCreate()

    val weatherDf = session
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/tmp/sample.csv")

    weatherDf
      .write
      .option("compression", "uncompressed")
      .parquet("/tmp/spark-parquet-output")

    weatherDf
      .write
      .option("header", "true")
      .csv("/tmp/spark-csv-output")

    weatherDf.write.saveAsTable("weather_stuff")

    session.sql("SELECT COUNT(date) FROM weather_stuff").show()

    session.stop()
  }

}
