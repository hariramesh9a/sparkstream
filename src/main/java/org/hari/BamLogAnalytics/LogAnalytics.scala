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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;

object LogAnalytics {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("BAMLogAnalytics").setMaster("local[*]")
     val ssc = new StreamingContext(conf, Seconds(2))
     ssc.checkpoint("C:/data/cehckpoint") 
    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFileStream("C:/data/test.dat")
    val words = lines.flatMap(_.split(" "))
    val common_errors=List("RDD","401","402","403")    
    val error_page = words.filter {error => common_errors exists (_ contains error) } 
    val error_pair=error_page.map { ipanderrors =>(ipanderrors.split(" ")(3),(ipanderrors.split(" ")(1-4)))  }
    val windowedWordCounts = error_pair.reduceByWindow((a,b)=>( a._1,a._2), (a,b)=>(a), Seconds(90), Seconds(20))    
    windowedWordCounts.print()
    windowedWordCounts.saveAsTextFiles("ts_", "errors")     
    ssc.start()
    ssc.awaitTermination()
  }

}