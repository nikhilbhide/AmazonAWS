package com.nik.spark

import org.apache.spark.rdd.RDD
import org.apache.commons.codec.digest.DigestUtils._
import org.apache.spark.HashPartitioner
import java.security.MessageDigest
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import scala.Iterator

/**
 * Calculates checksum of supplied RDD
 */
class CheckSumCalculator(rdd: RDD[String]) extends Serializable {

  /**
   * Calculates checksum for supplied RDD. Check sum is figured out by calculating md5 hash of RDD.
   * @param rdd The rdd of S3 file
   * @return The checksum of S3 file contents
   */
  def calculateCheckSum(parts: Int = 1000): String = {
    val partitioner = new HashPartitioner(parts)
    val output = rdd
      .map(x => (x, 1))
      .repartitionAndSortWithinPartitions(partitioner)
      .map(x => (x._1))
      .mapPartitions(x => Iterator(x.foldLeft(getMd5Digest())(md5)))
      .map(x => new java.math.BigInteger(1, x.digest()).toString(16))
      .collect()
      .sorted
      .foldLeft(getMd5Digest())(md5)
    val checksum = new java.math.BigInteger(1, output.digest()).toString(16)
    return (checksum)
  }

  /**
   * @param currMD5 The MD5 value of file contents so far read
   * @param input The next string in
   * @return The updated MD5 value
   */
  def md5(currMD5: MessageDigest, input: String): MessageDigest = {
    val inputBytes = input.getBytes("UTF-8")
    currMD5.update(inputBytes, 0, inputBytes.length)
    currMD5
  }
}