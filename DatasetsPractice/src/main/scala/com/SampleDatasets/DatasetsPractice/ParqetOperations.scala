package com.SampleDatasets.DatasetsPractice

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object ParqetOperations {
  def main(args: Array[String]){
    
    val conf = new SparkConf().setAppName("ParqetOperations").setMaster("local[*]").set("spark.cassandra.connection.host", "127.0.0.1")
    
    val sc = new SparkContext(conf)
   
    
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
//    val df = sqlContext.read.json("src/main/resources/people.json").write.parquet("src/main/resources/Parqet")
    
    val par = sqlContext.read.parquet("src/main/resources/Parqet")
    
    par.show()
    par.select("name", "age").show()
    
    
  }
}