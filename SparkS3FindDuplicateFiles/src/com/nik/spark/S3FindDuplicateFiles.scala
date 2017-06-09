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
import org.apache.spark.rdd.RDD

case class FileChecksum(checkSum: String, filePath: String)

/**
 * Traverses s3 either entirely including all buckets or for a bucket only based on provided bucket name.
 */
class S3FindDuplicateFiles(awsAccessKey: String, awsSecretKey: String) extends java.io.Serializable {
  val S3Scheme = "s3a://"
  var S3FileSeparator = "/"

  /**
   * Initializes Spark Session object and also configures aws access key and secret keys in spark context.
   *
   * @return spark The spark session instance
   */
  def initSpark(): SparkSession = {
    val spark = SparkSession
      .builder
      .appName("SparkS3FindDuplicateFiles")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", awsAccessKey)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", awsSecretKey)
    spark.sparkContext.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    spark
  }

  /**
   * Initializes s3 client for supplied access key and secret key.
   *
   * @return s3Client The s3Client instance
   */
  def initS3Client(): AmazonS3Client = {
    val credential = new BasicAWSCredentials(awsAccessKey, awsSecretKey)
    return new AmazonS3Client(credential)
  }

  def isRootPath(path: String): Boolean = {
    return path.equals("")
  }

  def isS3Directory(path: String): Boolean = {
    return path.endsWith("/")
  }

  implicit def rddToMD5Functions(rdd: RDD[String]) = new CheckSumCalculator(rdd)

  /**
   * It traverses entire s3, all buckets and explores all possible routes, while exploring it stores all paths as well
   *
   * @param s3Paths The list in which all routes are stored.
   */
  def exploreS3(s3Paths: ListBuffer[String], inputBuckets: List[String]) {
    val s3Client = initS3Client()
    var buckets = s3Client.listBuckets()

    if (!inputBuckets.isEmpty)
      buckets = buckets.filter { bucket => inputBuckets.contains(bucket.getName) }

    buckets.toSeq.foreach { bucket =>
      var s3Objects = S3Objects.withPrefix(s3Client, bucket.getName, "")
      for (s3Object <- s3Objects) {
        if (!isS3Directory(s3Object.getKey)) {
          var absoluteS3Path = bucket.getName().concat(S3FileSeparator).concat(s3Object.getKey)
          s3Paths += absoluteS3Path
        }
      }
    }
  }

  /**
   * Reads all files in the file path using Spark, distributes this to entire cluster, calculates checksum of each of the file using MD5.
   * Groups files based on checksum and find the corresponding count.
   * Order the result based on the duplication count.
   *
   * @param s3Paths The list of all s3 paths
   */
  def checkDuplicateFiles(s3Paths: ListBuffer[String]) {
    val spark = initSpark()
    import spark.implicits._

    val resultList = new ListBuffer[FileChecksum]
    s3Paths.foreach(filePath => {
      val fileRDD = initSpark().sparkContext.textFile(S3Scheme.concat(filePath))
      fileRDD.cache()

      val checkSum = fileRDD.calculateCheckSum(100)

      val result = FileChecksum(checkSum, filePath)
      resultList += result
    })

    val filePathByCheckSumDS = spark.sparkContext.parallelize(resultList).toDS()

    val duplicateFiles = filePathByCheckSumDS.select($"checkSum", $"filePath")
      .groupBy($"checkSum")
      .agg(count($"filePath") as "duplicateFileCount", collect_list($"filePath") as "duplicateFilePaths")
      .filter($"duplicateFileCount" > 1)
      .sort($"duplicateFileCount".desc)

    //display results
    duplicateFiles.show()
  }
}