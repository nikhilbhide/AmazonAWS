package com.nik.spark

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._

/**
 * Traverses s3 or provided bucket and explores files and folders, reads files using spark and displays all paths.
 */

object S3FindDuplicateFilesTest {
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

		val sparkS3FindDuplicateFilesInstance = new S3FindDuplicateFiles(args(0),args(1))
		sparkS3FindDuplicateFilesInstance.initS3Client()
		val s3Paths = new ListBuffer[String]()
		if(args.length>2) {
			val bucketName = args(3)
					var path = S3BucketDefaultPath
					if(args.length==4) {
						path = args(4)
					}
			sparkS3FindDuplicateFilesInstance.exploreS3(args(2), path,s3Paths)
		}
		else {
			sparkS3FindDuplicateFilesInstance.exploreS3(s3Paths)	
		}		
	}
}