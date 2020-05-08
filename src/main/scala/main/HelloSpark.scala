package main

import org.apache.spark.sql.SparkSession

object HelloSpark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Hello Spark App").getOrCreate()

    val textDf = spark.read.text(s"${System.getProperty("user.home")}/xeroque-holmes.txt")

    textDf.printSchema

    textDf.show

    println("Hello Spark")
    spark.stop()
  }

}
