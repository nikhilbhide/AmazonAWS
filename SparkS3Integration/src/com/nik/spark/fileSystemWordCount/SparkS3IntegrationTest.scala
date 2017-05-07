package com.nik.spark.fileSystemWordCount

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.{DataFrame, Row, SQLContext, DataFrameReader}
import org.apache.spark.{InterruptibleIterator, TaskContext, Partition, SparkContext}
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary
import scala.collection.JavaConversions._
import com.amazonaws.auth.{DefaultAWSCredentialsProviderChain, BasicAWSCredentials}
import com.amazonaws.services.s3.iterable.S3Objects
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.sql.functions._

/**
 * Traverses s3 or provided bucket and explores files and folders, reads files using spark and displays all paths.
 */
object SparkS3IntegrationTest {
	var S3BucketDefaultPath = ""
			/**
			 * Displays all paths in s3 or a particular bucket.
			 * 
			 * @param s3Paths The list of all possible paths 
			 */
			def displayS3Traversal(s3Paths:ListBuffer[String]) {
		for(s3Path<-s3Paths) {
			println(s"S3 path is ${s3Path}")
		}
	}

	/**
	 * Accepts AWS access key and secret key and explores entire s3.
	 * It can also traverse particular bucket, provided bucket name is supplied in the JVM argument list. 
	 *  
	 * @param args Input arguments, first argument is aws access key and second argument is aws secret key; these two inputs are mandatory. 
	 *             Third (optional) argument is bucket name and fourth (optional) argument is path inside bucket. 
	 *   
	 */
	def main(args:Array[String]) {
		if(args.length!=2) {
			println("AWS access key and secret key are not provided. Provide access key as first argument and secret key as access key.")
			System.exit(0)
		}

		val sparkS3Instance = new SparkS3Integration(args(0),args(1))
		sparkS3Instance.initS3Client()
		val s3Paths = new ListBuffer[String]()
		if(args.length>2) {
			val bucketName = args(3)
					var path = S3BucketDefaultPath
					if(args.length==4) {
						path = args(4)
					}
			sparkS3Instance.traverseS3(args(2), path,s3Paths)
		}
		else {
			sparkS3Instance.traverses3(s3Paths)
			displayS3Traversal(s3Paths)			
		}		
	}
}