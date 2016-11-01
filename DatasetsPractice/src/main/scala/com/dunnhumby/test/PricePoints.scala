package com.dunnhumby.test

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset

object PricePoints {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local").set("spark.cassandra.connection.host", "127.0.0.1")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val prods: Dataset[Products] = sqlContext.read.json("src/main/resources/ProductPrices.json").as[Products]

    prods.foreach { x =>

      val avg = ((x.MaximumPrice + x.MaximumPrice) / 2);
      val avg2 = BigDecimal(avg).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble;
      
      println("------------------"+avg2)

      val finalAvg = ((avg2 * 100) % 10).toInt;

      if (finalAvg == 3) {
        println(x.ProductName + "," + x.OriginalPrice + "," + x.MaximumPrice + "," + x.MaximumPrice + "," + avg2)
      } else if (finalAvg == 5) {
        println(x.ProductName + "," + x.OriginalPrice + "," + x.MaximumPrice + "," + x.MaximumPrice + "," + avg2)
      } else if (finalAvg == 9) {
        println(x.ProductName + "," + x.OriginalPrice + "," + x.MaximumPrice + "," + x.MaximumPrice + "," + avg2)
      } else { println(x.ProductName + "," + x.OriginalPrice + "," + x.MaximumPrice + "," + x.MaximumPrice + ",No Price point within min/max range") }

    }

   /* val prodRdd = sc.textFile("src/main/resources/Products.txt")
    
    
    prodRdd.map(a => a.split(",")).foreach(println)
    println("----------------------------------------------------")
    prodRdd.flatMap{a => 
      val set = a.split(",").toList;
      set.indexWhere { x => ??? }
      
    }*/


  }
}

case class Products(ProductName: String, OriginalPrice: Double, MinimumPrice: Double, MaximumPrice: Double)

