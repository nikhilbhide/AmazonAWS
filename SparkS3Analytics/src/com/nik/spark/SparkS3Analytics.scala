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

/**
 * Performs analytics of amazon aws s3 by traversing s3 either entirely including all buckets or for a bucket only based on provided bucket name.
 * As a part of analytics, it provides insights for all buckets or a specific bucket->
 * 1. # of directories in s3
 * 2. Files in s3 by format
 * 3. duplicate files
 * 
 */
 case class DuplicateFileResult(checkSum: String, filePath: String)
  case class FilesByFormatResult(bucketName:String,format:String,filePath:String)
  case class DirectoriesResult(bucketName:String,filePath:String)  
class SparkS3Analytics(awsAccessKey: String, awsSecretKey: String) extends java.io.Serializable {
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
      .appName("SparkS3Integration")
      .master("local[*]")
      .getOrCreate()
         spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", awsAccessKey)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", awsSecretKey)
    spark.sparkContext.hadoopConfiguration.set("org.apache.hadoop.fs.s3a.S3AFileSystem", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    spark
  }

  /**
   * Initializes s3 client for supplied access key and secret key.
   *
   * @return s3Client The s3Client instance
   */
  def initS3Client(): AmazonS3Client = {
    val credential = new BasicAWSCredentials(awsAccessKey, awsSecretKey)
    val s3Client = new AmazonS3Client(credential)
    s3Client
  }

  def isRootPath(path: String): Boolean = {
    return path.equals("")
  }

  def isS3Directory(path: String): Boolean = {
    return path.endsWith("/")
  }
  
  implicit def rddToMD5Functions(rdd: RDD[String]) = new CheckSumCalculator(rdd)

  /**
   * Performs analytics of amazon aws s3 by traversing s3 either entirely including all buckets or for a bucket only based on provided bucket name.
   *
   */
  def performS3Analytics() {
    var s3Paths = new ListBuffer[String]
    var s3Directories = new ListBuffer[DirectoriesResult]
    val s3Client = initS3Client()
    
    var buckets = s3Client.listBuckets()
   // buckets.toSeq.foreach { bucket =>
      var s3Objects = S3Objects.withPrefix(s3Client, "mysimbucket", "TestFolder/")
      for (s3Object <- s3Objects) {
        exploreS3(s3Object.getBucketName(), s3Object.getKey, s3Paths,s3Directories)
      }
      
     checkDuplicateFiles(s3Paths)
     s3AnalyticsByFileFormats(s3Paths)
     s3DirectoryAnalytics(s3Directories)
     s3AnalyticsByFileFormats(s3Paths)
  }
  
  /**
   * Performs analytics of s3 directories of entire s3 or a specific bucket
   *
   * @param s3Directories The directories list
   * 
   */
  def s3DirectoryAnalytics(s3Directories:ListBuffer[DirectoriesResult]) {
    val spark = initSpark()
    import spark.implicits._
    println("directories list is :")
    s3Directories.foreach { directoryName => println(directoryName) }
    val directoriesDS = spark.sparkContext.parallelize(s3Directories).toDS()
    directoriesDS.cache()
    directoriesDS.createOrReplaceTempView("directories")
    val directoriesCountPerBucket = spark.sqlContext.sql("select bucketName,count(filePath) from directories group by bucketName").toJSON.collect()
    val totalDirectoriesCount = spark.sqlContext.sql("select count(filePath) from directories")
    
    println(s"# of directories in explored s3 path is ${totalDirectoriesCount}")
    directoriesCountPerBucket.foreach(row=>println(row))
  }

  /**
   * Performs analytics of s3 file formats for a bucket or all buckets
   *
   * @param s3Paths The s3 file paths
   * 
   */
  def s3AnalyticsByFileFormats(s3Paths: ListBuffer[String]) {
    s3Paths.foreach(filePath=>println(filePath))
    val spark = initSpark()
    val filePathsRDD = spark.sparkContext.parallelize(s3Paths)
    val s3AnalyticsByFileFormats = filePathsRDD.map(filePath=> {
      println("here is file path" +filePath)
      val fileFormat = filePath.split("\\.")(1)
      (fileFormat,1)})
      .countByKey()     
    s3AnalyticsByFileFormats.foreach(record=> {
      println(s"There are ${record._2} files having file format ${record._1}" )
    })
  }
  
  /**
   * It traverses bucket based on bucket name and prefix. While exploring it stores all paths as well. If any file is encountered then it uses spark to read the file.
   *
   * @param bucketName The bucket that is to be traversed
   * @param path The prefix
   * @param s3Paths The list in which all routes are stored
   * @param s3Directories The list in which all directories are stored
   * 
   */
  def exploreS3(bucketName: String, path: String, s3Paths: ListBuffer[String], s3Directories:ListBuffer[DirectoriesResult]) {
    val s3Client = initS3Client()
    if (isRootPath(path)) {
      var s3Objects = S3Objects.withPrefix(s3Client, bucketName, path)
      for (s3Object <- s3Objects) {
        exploreS3(s3Object.getBucketName(), s3Object.getKey(), s3Paths,s3Directories)
      }
    } else if (!isS3Directory(path)) {
      var absoluteS3Path = bucketName.concat(S3FileSeparator).concat(path)
      s3Paths += absoluteS3Path
    }
    else if(isS3Directory(path)) {
      s3Directories+=DirectoriesResult(bucketName,path)
    }
  }

  /**
   * Checks whether s3 has duplicate files or not.
   * Reads all files in the file path using Spark, distributes this to entire cluster, calculates checksum of each of the file using MD5.
   * Groups files based on checksum and find the corresponding count.
   * Order the result based on the # of duplication.
   * If recursive traverse is true then all files under supplied file path are considered for duplicate check.
   *
   * @param s3Paths The list of all s3 paths
   */
    def checkDuplicateFiles(s3Paths: ListBuffer[String]) {
    val spark = initSpark()
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val duplicateFileResult = new ListBuffer[DuplicateFileResult]
    
    s3Paths.foreach(filePath => {
      val fileRDD = initSpark().sparkContext.textFile(S3Scheme.concat(filePath))
      val checkSum = fileRDD.calculateCheckSum(10)
      val result: DuplicateFileResult = DuplicateFileResult(checkSum, filePath)
      duplicateFileResult += result
    })

    val  duplicateFileResultDS = spark.sparkContext.parallelize(duplicateFileResult).toDS()
    //dplicateFileResultDS.createOrReplaceTempView("duplicateFiles")
    
    val results = duplicateFileResultDS.select($"checkSum",$"filePath")
                                       .groupBy($"checkSum")
                                       .agg(count(duplicateFileResultDS("filePath")) as "duplicateFileCount",collect_list(duplicateFileResultDS("filePath")) as "duplicateFilePaths")
                                       .sort($"duplicateFileCount".desc)
                                       .toJSON.collect()
                                  
     //display results
     results.foreach(row=>println(row)) 
    }
}