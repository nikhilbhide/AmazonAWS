package com.nik.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.mutable.ListBuffer
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import scala.collection.mutable.ListBuffer
import com.amazonaws.services.s3.AmazonS3Client
import scala.collection.JavaConversions._
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.iterable.S3Objects
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import com.nik.spark.Utilities._
import java.util.regex.Pattern
import java.util.regex.Matcher
import java.sql.Date;
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import scala.collection.mutable._
import com.sun.org.apache.xalan.internal.xsltc.compiler.ValueOf
import org.apache.spark.sql.Dataset

case class S3LogEntry(ownerName:String,bucketName:String,dateTime:Date,ipAddress:String,requester:String,requesterID:String,
      operation:String,key:String,requesterURI:String,httpStatus:Int, errorCode:Int,bytesSent:Long,objectSize:Long,totalTime:Long,turnAroundTime:Long,referrer:String,userAgent:String,versionID:String)
      
object S3LogParserAnalytics {
  def getBuckets(s3LogsDS:Dataset[S3LogEntry]):List[String] =  {
    val buckets = s3LogsDS.select("bucketName")
				                      .distinct()               
				                      .agg(collect_list(s3LogsDS("bucketName") as "bucketsList"))
				                      
	  return buckets.collectAsList()
	                .map(row => row.toString())
	                .toList
	}
  
  def main(args: Array[String]) {
		val spark = SparkSession
				.builder
				.appName("SparkS3Integration")
				.master("local[*]")
				.getOrCreate()
				import spark.implicits._
  			val s3LogsRDD = spark.sparkContext.textFile("/home/nik/Downloads/s3LogFile.txt")

				val s3LogsDS = s3LogsRDD.map(parseLog)
                                .toDS()
				s3LogsDS.cache()
				        				    
				s3LogsDS.createOrReplaceTempView("s3Logs")
				val uniqueBuckeList = getBuckets(s3LogsDS)			
				val uniqueBucketNames = uniqueBuckeList.mkString(",")

        val bucketNames = spark.sparkContext.broadcast(uniqueBucketNames)
        // find out top n buckets by object size
				var n = 5
				val topNBucketsByObjectSize = spark.sqlContext.sql("select bucketName,Size from (select bucketName,sum(objectSize) as Size from s3Logs group by bucketName) order by Size")
				topNBucketsByObjectSize.show()
				
				//find out top n files per bucket 
				//need to use dens_rank() function
				n = 2
				val topNObjectsPerBucket = spark.sqlContext.sql("select bucketName,key,objectsize from (select bucketName,key,objectsize,dense_rank() OVER (PARTITION BY bucketName ORDER BY objectSize DESC) as rank from s3Logs) where rank <=2")  
				topNObjectsPerBucket.show()
				
			 //val topNIPAddressOverall
				n = 3
				var inputBucketName = "all"
				inputBucketName match {
				case "all" => {
				  inputBucketName = bucketNames.value
				}
				  
				}
				val topNIPAddressesOverall = spark.sqlContext.sql("select ipAddress,count(ipAddress) as Hits from s3Logs group by ipAddress having ipAddress in (select ipAddress from s3Logs where bucketName in ('$(inputBucketName)'))")
				//select bucketName,ipAddress,Hits from (
				topNIPAddressesOverall.show()
				// having bucketName in $(inputBucketName)
				val totalHitsAndBytesPerFile = spark.sqlContext.sql("select bucketName,key,count(key) as Hits, sum(objectsize) as Bytes from s3Logs where key!='error' and key!='-' group by bucketName,key")
				totalHitsAndBytesPerFile.show()
				
				val wSpec1 = Window.partitionBy("bucketName").orderBy("objectsize")
				val kl = s3LogsDS.filter(record=>record.bytesSent!=(-999))
				                 .withColumn("PercentageOfTotalPerBucket",sum(s3LogsDS("objectsize")).over(wSpec1)*100/s3LogsDS("objectsize"))
				                 
				kl.createOrReplaceTempView("Buckets")
			  spark.sqlContext.sql("select bucketName,objectsize,percentageOfTotalPerBucket as percentageOfTotalPerBucket from Buckets").show()    
				
				//    s3LogsDS.filter(logRecord=>logRecord.dateTime.equals("-"))
				//            .map(logRecord=> {
				//              val dateTime = logRecord.dateTime
				//              dateTime = dateTime.substring(1,dateTime.length()-1)
	}
}