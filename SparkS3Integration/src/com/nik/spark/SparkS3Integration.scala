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
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._

/**
 * Traverses s3 either entirely including all buckets or for a bucket only based on provided bucket name.  
 */
class SparkS3Integration(awsAccessKey:String,awsSecretKey:String) extends java.io.Serializable {
	var s3Client:AmazonS3Client = null
	val S3Scheme = "s3n://"
	var S3FileSeparator = "/"

			/**
			 * Initializes Spark Session object and also configures aws access key and secret keys in spark context. 
			 * 
			 * @return spark The spark session instance
			 */
			def initSpark():SparkSession = {
					val spark = SparkSession
							.builder
							.appName("SparkS3Integration")
							.master("local[*]")
							.getOrCreate()
							spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", awsAccessKey)
							spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey",awsSecretKey)
							spark
			}

			/**
			 * Initializes s3 client for supplied access key and secret key.
			 * 
			 * @return s3Client The s3Client instance
			 */
			def initS3Client():AmazonS3Client = {
					val credential = new BasicAWSCredentials(awsAccessKey,awsSecretKey)
					s3Client = new AmazonS3Client(credential)
					s3Client
			}

			def isRootPath(path:String): Boolean = {
					return path.equals("")
			}

			def isS3Directory(path:String): Boolean = {
					return path.endsWith("/")
			}

			/**
			 * It traverses entire s3, all buckets and explores all possible routes, while exploring it stores all paths as well
			 * 
			 * @param s3Paths The list in which all routes are stored.
			 */
			def traverses3(s3Paths:ListBuffer[String]) {
				var buckets = s3Client.listBuckets()
						buckets.toSeq.foreach { bucket => 
						var s3Objects = S3Objects.withPrefix(s3Client,bucket.getName(),"")
						for(s3Object <- s3Objects) {
							traverseS3(s3Object.getBucketName(),s3Object.getKey,s3Paths)
						}
				}
			}

			/**
			 * It traverses bucket based on bucket name and prefix. While exploring it stores all paths as well. If any file is encountered then it uses spark to read the file.
			 * 
			 * @param bucketName The bucket that is to be traversed
			 * @param path The prefix
			 * @param s3Paths The list in which all routes are stored
			 */
			def traverseS3(bucketName:String, path:String,s3Paths:ListBuffer[String]) {
				if(isRootPath(path)) {
					var s3Objects = S3Objects.withPrefix(s3Client,bucketName,path)
							for(s3Object <- s3Objects) {
								traverseS3(s3Object.getBucketName(),s3Object.getKey(),s3Paths)
							}
				}
				else if (!isS3Directory(path)) {
					var absoluteS3Path = bucketName.concat(S3FileSeparator).concat(path)
							s3Paths+=absoluteS3Path
							readS3Files(absoluteS3Path)
				}
			}

			/**
			 * Read s3 file using spark. It uses s3n which is s3 native scheme to access file in s3 
			 * 
			 * @param path The s3 file path to be read
			 * 
			 */
			def readS3Files(path:String) {
				val spark = initSpark()
						import spark.implicits._
						val fileRDD = initSpark().sparkContext.textFile(S3Scheme.concat(path)).toDS()
						fileRDD.foreach(record=>println(record))  
			}
}