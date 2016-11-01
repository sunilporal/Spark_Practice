package com.SampleDatasets.DatasetsPractice

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.datastax.spark.connector._

object PeopleDataset {
  def main(args:Array[String]) = {
    
    val conf = new SparkConf().setAppName("DataFrameOperations").setMaster("local").set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(conf)
   
    
    val sqlContext = new SQLContext(sc)
 //   import sqlContext.implicits._
    
    val df = sqlContext.read.json("src/main/resources/people.json")
    
    df.select ("id", "name", "age")
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "people", "keyspace" -> "sparkpractice"))
      .save()
      
  }
}