package main

import org.apache.spark.sql.{DataFrame, SparkSession}

object HelloSpark {

  private def SampleTextPath = "/tmp/sample.txt"

  private def SampleCsvPath = "/tmp/sample.csv"

  private def SampleParquetPath = "/tmp/sample.parquet"

  private def readUnstructuredText(spark: SparkSession) = spark
    .read
    .text(SampleTextPath)

  private def readCsv(spark: SparkSession) = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(SampleCsvPath)

  private def readParquet(spark: SparkSession) = spark
    .read
    .option("inferSchema", "true")
    .parquet(SampleParquetPath)

  private def showSchemaAndRecords(dataFrame: DataFrame) {
    dataFrame.printSchema()
    dataFrame.show(30)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Hello Spark App").getOrCreate()

    val textDf = readUnstructuredText(spark)
    val csvDf = readCsv(spark)
    val parquetDf = readParquet(spark)

    Seq(textDf, csvDf, parquetDf).foreach(showSchemaAndRecords)

    spark.stop()
  }

}
