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
import org.apache.spark.sql.Dataset
import org.apache.spark.broadcast.Broadcast

case class S3LogEntry(ownerName: String, bucketName: String, dateTime: Date, ipAddress: String, requester: String, requesterID: String,
		operation: String, key: String, requesterURI: String, httpStatus: Int, errorCode: Int, bytesSent: Long, objectSize: Long, totalTime: Long, turnAroundTime: Long, referrer: String, userAgent: String, versionID: String, country:String, city:String,longitude:Float,latitude:Float)


		object S3LogParserAnalytics {
	var s3LogsDS:Dataset[S3LogEntry] = null
	
			def initSpark(): SparkSession = {
					println("here")
					val spark = SparkSession
					.builder
					.appName("SparkS3Integration")
					.master("local[*]")
					.getOrCreate()
					import spark.implicits._
					spark.getClass
					return spark
			}

			def getBuckets(): List[String] = {
					val buckets = s3LogsDS.rdd
							.map(row => ("'").concat(row.bucketName).concat("'"))
							.distinct
							.collect()
							return buckets.toList
			}

			def findTopNBucketsByObjectSize(n: Int) {
				println(n)
				val topNBucketsByObjectSize = initSpark().sqlContext.sql(s"select bucketName,Size from (select bucketName,sum(objectSize) as Size from s3Logs group by bucketName) order by Size DESC limit $n")
				val spark = initSpark()
				import spark.implicits._

				topNBucketsByObjectSize.createOrReplaceTempView("topNBucketsyObjectSize")
				topNBucketsByObjectSize.show()
			}

			//find out top n files per bucket 
			//need to use dens_rank() function
			def findTopNObjectForSelectedBuckets(inputBucketName: String, n: Int) {
				val topNObjectsPerBucket = initSpark().sqlContext.sql(s"select bucketName,key,objectsize from (select bucketName,key,objectsize,dense_rank() OVER (PARTITION BY bucketName ORDER BY objectSize DESC) as rank from s3Logs where bucketName in ($inputBucketName))")
						topNObjectsPerBucket.createOrReplaceTempView("topNObjects")
						topNObjectsPerBucket.show()
			}

			def getTopNIPAddressForSelectedBuckets(inputBucketName: String, n: Int) {
			  val topNIPForSelectedBuckets = initSpark().sqlContext.sql(s"select ipAddress,count(ipAddress),country,city,latitude,longitude as Hits from s3Logs group by ipAddress,country,city,latitude,longitude having ipAddress in (select ipAddress from s3Logs where bucketName in ($inputBucketName))")
  			topNIPForSelectedBuckets.show()
			}

			def getTotalHitsPerFileForSelectedBuckets(inputBucketName: String) {
				val totalHitsPerFileForSelectedBuckets = initSpark().sqlContext.sql(s"select bucketName,key,count(key) as Hits, sum(objectsize) as Bytes from s3Logs where key!='error' and key!='-' group by bucketName,key having bucketName in ($inputBucketName) order by bucketName,key,Hits DESC")
						totalHitsPerFileForSelectedBuckets.show()
			}

			def getObjectsWithPercentageToTotalForSelectedBuckets(inputBucketName: String, s3LogsDS: Dataset[S3LogEntry]) {
				val wSpec = Window.partitionBy("bucketName").orderBy("objectsize")
						val objectsWithPercentageToTotal = s3LogsDS.filter(record => record.bytesSent != (-999))
						.withColumn("PercentageOfTotalPerBucket", sum(s3LogsDS("objectsize")).over(wSpec) * 100 / s3LogsDS("objectsize"))
						objectsWithPercentageToTotal.createOrReplaceTempView("Buckets")
						initSpark().sqlContext.sql(s"select bucketName,objectsize,percentageOfTotalPerBucket as percentageOfTotalPerBucket from Buckets where bucketName in ($inputBucketName)").show()
			}

			def getS3LogsDS():Dataset[S3LogEntry] = {
					return s3LogsDS
			}

			def loadData(filePath:String):Dataset[S3LogEntry] = {
					val spark = initSpark()
							import spark.implicits._
							val s3LogsRDD = spark.sparkContext.textFile(filePath)
							val s3LogsDS = s3LogsRDD.map(parseLog)
							.toDS()
							s3LogsDS.cache()
							s3LogsDS.createOrReplaceTempView("s3Logs")
							return s3LogsDS
			}

			def main(args: Array[String]) {       
				s3LogsDS = loadData("/home/nik/Downloads/s3LogFile.txt")
						val uniqueBuckeList = getBuckets()
						val uniqueBucketNames = uniqueBuckeList.mkString(",")
						val bucketNames = initSpark().sparkContext.broadcast(uniqueBucketNames)
						var n = 5
						var inputBucketName = "all"
						inputBucketName match {
						case "all" => {
							inputBucketName = bucketNames.value
						}
				}
		    findTopNBucketsByObjectSize(n)
		  	findTopNObjectForSelectedBuckets(inputBucketName,n)
		 		getTotalHitsPerFileForSelectedBuckets(inputBucketName)
				getObjectsWithPercentageToTotalForSelectedBuckets(inputBucketName,s3LogsDS)
				getTopNIPAddressForSelectedBuckets(inputBucketName, n)
			}
}