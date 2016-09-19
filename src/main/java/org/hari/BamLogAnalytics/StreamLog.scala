package org.hari.BamLogAnalytics
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.{ Row, SQLContext }
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{ StructType, DateType, StringType, StructField, DoubleType, TimestampType, IntegerType }
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import scala.io.Source
import java.io.PrintWriter
import java.io.File
import org.apache.spark.sql.hive.HiveContext

class StreamLog {
  
  
  def main(args: Array[String]): Unit = {
    
  val conf = new SparkConf().setAppName("BAMLogAnalytics").setMaster("local[*]")
  val ssc = new StreamingContext(conf, Seconds(10))
  val fstream= ssc.fileStream("C:/Users/ramesh2/My Documents/SametimeFileTransfers/ex160715/ex160715.log")
   
  
  println(fstream.flatMap(_.toString()))
  
  ssc.awaitTermination()  // Wait for the computation to terminate
  ssc.stop()
  
  }
  
  
  
  
}