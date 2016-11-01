package com.SampleDatasets.DatasetsPractice

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.Dataset

object DataFramesPractice {
  
  def main(args:Array[String]) = {
    
    val conf = new SparkConf().setAppName("WordCount").setMaster("local").set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(conf)
    
    
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    
    
    val df = sqlContext.read.json("src/main/resources/sample.json")

    df.select ("id", "approvalfy", "board_approval_month", "borrower", "countrycode", "countryshortname", "project_name", "status", "url")
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "samplepersons", "keyspace" -> "sparkpractice"))
      .save()
      

   
    
    
    
    
  }
    

  }

