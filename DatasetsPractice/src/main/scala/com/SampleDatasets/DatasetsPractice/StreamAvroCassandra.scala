package com.SampleDatasets.DatasetsPractice

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

import kafka.serializer.StringDecoder
import org.apache.spark.sql.internal.SQLConf

object StreamAvroCassandra {
  
  val conf = new SparkConf().setAppName("StreamAvroCassandra").setMaster("local[*]").set("spark.cassandra.connection.host", "127.0.0.1")
    conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  def main(args: Array[String]) {  

    
println("-------------------Test --------------------------------------")
    // Declaring values for Spark Streaming from Kafka Topics
    val ssc = new StreamingContext(sc, Seconds(5)) // Streaming context object to stream every 5 seconds
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092") // setting kafka host
    val topics = Set("CustomerLogins") // Set of Topics to consume
    ssc.checkpoint("/home/sac/work/KafkaCheckPoint") // defining Check point directory
println("-------------------Test 111--------------------------------------")   
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)

    stream.checkpoint(Seconds(2 * 5))

   
    stream.foreachRDD(rdd => sqlContext.read.json(rdd)
        .select("id", "name", "age")
        .write
        .format("org.apache.spark.sql.cassandra")
        .options(Map("table" -> "people", "keyspace" -> "sparkpractice"))
        .mode(SaveMode.Append)
        .save())
//  stream.foreachRDD(rdd => rdd.collect().foreach(println) )
    
    ssc.start()
    println("started now-->> " + compat.Platform.currentTime)
    ssc.awaitTermination()

  }
  
  

}