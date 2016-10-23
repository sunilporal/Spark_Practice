package com.SampleDatasets.DatasetsPractice

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset

object DatesetsOperations {

  def main(args: Array[String]) = {

    val conf = new SparkConf().setAppName("WordCount").setMaster("local").set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val ds = Seq(1, 2, 3).toDS()
    ds.map(_ + 1).foreach { println(_) }
    
    
    val df = sqlContext.read.json("src/main/resources/people.json")
    val pplDS: Dataset[Person] = df.as[Person]
    
    pplDS.show()
    

  }
}

