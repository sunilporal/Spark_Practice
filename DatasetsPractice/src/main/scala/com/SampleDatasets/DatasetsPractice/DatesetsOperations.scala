package com.SampleDatasets.DatasetsPractice

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import com.datastax.spark.connector._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.SaveMode

object DatesetsOperations {
  val conf = new SparkConf().setAppName("DatasetsOperations").setMaster("local[*]").set("spark.cassandra.connection.host", "127.0.0.1")
  val sc = new SparkContext(conf)

  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._

  def main(args: Array[String]) = {

    val peoplePath = "src/main/resources/people.json"
    val parquetPath = "src/main/resources/SampleParq*"

    insertParquetFileData(parquetPath)

  }

  def insertDataFrameToTable(path: String) = {
    val ds = Seq(1, 2, 3).toDS()
    ds.map(_ + 1).foreach { println(_) }

    val df = sqlContext.read.json(path)
    val pplDS: Dataset[Person] = df.as[Person]

    pplDS.show()

    val ppl = sc.cassandraTable("sparkpractice", "samplepersons")
    println("Total number of rows in table samplepersons : " + ppl.count())

    println(ppl.columnNames)
  }

  def insertParquetFileData(path: String) = {

    val par: Dataset[SamplePerson] = sqlContext.read.parquet(path).as[SamplePerson]

    par.select("id", "approvalfy", "board_approval_month", "borrower", "countrycode", "countryshortname", "project_name", "status", "url")
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "samplepersons", "keyspace" -> "sparkpractice"))
      .mode(SaveMode.Append)
      .save()
      
    

  }
}
case class Person(id: Long, name: String, age: Long)

case class SamplePerson(id: String, approvalfy: String, board_approval_month: String, borrower: String, countrycode: String,
                        countryshortname: String, project_name: String, status: String, url: String)
