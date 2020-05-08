package main

import org.apache.spark.sql.SparkSession

object HelloSpark {

  private def SampleTextPath = "/tmp/sample.txt"

  private def SampleCsvPath = "/tmp/sample.csv"

  private def readUnstructuredText(spark: SparkSession) = spark
    .read
    .text(SampleTextPath)

  private def readCsv(spark: SparkSession) = spark
    .read
    .option("header", "true")
    .csv(SampleCsvPath)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Hello Spark App").getOrCreate()

    val textDf = readUnstructuredText(spark)
    val csvDf = readCsv(spark)

    textDf.printSchema()
    textDf.show(30)

    csvDf.printSchema()
    csvDf.show(30)

    spark.stop()
  }

}
