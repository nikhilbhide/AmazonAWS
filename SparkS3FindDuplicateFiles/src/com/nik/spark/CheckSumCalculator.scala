package com.nik.spark

import org.apache.spark.rdd.RDD
import org.apache.commons.codec.digest.DigestUtils._
import org.apache.spark.HashPartitioner
import java.security.MessageDigest
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import scala.Iterator

/**
 * Calculates checksum for supplied RDD. Check sum is figured out by calculating md5 hash of RDD.
 */
class CheckSumCalculator(rdd: RDD[String]) extends Serializable {

  def calculateCheckSum(parts: Int = 1000): String = {
    val partitioner = new HashPartitioner(parts) 
    val output = rdd.
               map(x => (x, 1)).
               repartitionAndSortWithinPartitions(partitioner).
               map(_._1).
               mapPartitions(x => Iterator(x.foldLeft(getMd5Digest())(md5))).
               map(x => new java.math.BigInteger(1, x.digest()).toString(16)).
               collect().
               sorted.
               foldLeft(getMd5Digest())(md5)
    val checksum = new java.math.BigInteger(1, output.digest()).toString(16)
    return(checksum)
  }

 /**
  * @param prev
  * @param in2
  * @return
  */
  def md5(prev: MessageDigest, next: String): MessageDigest = {
    val b = next.getBytes("UTF-8")
    prev.update(b, 0, b.length)
    prev
  }
}