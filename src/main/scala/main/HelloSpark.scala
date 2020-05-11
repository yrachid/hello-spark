package main

import org.apache.spark.sql.{DataFrame, SparkSession}

object HelloSpark {

  private val sampleTextPath = "/tmp/sample.txt"

  private val sampleCsvPath = "/tmp/sample.csv"

  private val sampleParquetPath = "/tmp/sample.parquet"

  private def sampleRemoteText(bucketName: String) = s"s3a://$bucketName/sample.txt"

  private def readUnstructuredText(spark: SparkSession) = spark
    .read
    .text(sampleTextPath)

  private def readCsv(spark: SparkSession) = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(sampleCsvPath)

  private def readParquet(spark: SparkSession) = spark
    .read
    .option("inferSchema", "true")
    .parquet(sampleParquetPath)

  private def readRemoteText(spark: SparkSession, bucketName: String) = spark
    .read
    .text(sampleRemoteText(bucketName))

  private def showSchemaAndRecords(dataFrame: DataFrame) {
    dataFrame.printSchema()
    dataFrame.show(30)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Hello Spark App").getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", System.getenv("AWS_ACCESS_KEY_ID"))
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", System.getenv("AWS_SECRET_KEY"))

    val textDf = readUnstructuredText(spark)
    val csvDf = readCsv(spark)
    val parquetDf = readParquet(spark)
    val remoteTextDf = readRemoteText(spark, System.getenv("HELLO_SPARK_AWS_BUCKET_NAME"))

    Seq(textDf, csvDf, parquetDf, remoteTextDf).foreach(showSchemaAndRecords)

    spark.stop()
  }

}
