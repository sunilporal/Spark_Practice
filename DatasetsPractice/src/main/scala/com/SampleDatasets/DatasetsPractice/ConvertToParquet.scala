package com.SampleDatasets.DatasetsPractice

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.sql.SQLClientInfoException
import org.apache.spark.sql.SQLContext

object ConvertToParquet {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("ConvertToParquet").setMaster("local[*]").set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    // converting JSON to DataFrame file
    //   val df = sqlContext.read.json("src/main/resources/sample_part1.json")

    //Converting DataFrame to Parquet
    //   val par = df.write.parquet("src/main/resources/SampleParqet1")
    
    sqlContext.read.json("src/main/resources/sample_part1.json").write.parquet("src/main/resources/SampleParqet1")

    sqlContext.read.json("src/main/resources/sample_part2.json").write.parquet("src/main/resources/SampleParqet2")

  }
}