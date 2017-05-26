package com.nik.spark

import java.util.regex.Pattern
import java.util.regex.Matcher
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

object Utilities {
  
  /** Retrieves a regex Pattern for parsing s3 logs. */
  def s3LogPattern(): Pattern = {
    val ownerName = "(\\S+)"
    val bucketName = "(\\S+)"
    val dateTime = "(\\[.+?\\])"
    val ip = "($ddd\\.$ddd\\.$ddd\\.$ddd)?"
    val requester = "(\\S+)"
    val requesterID = "(\\S+)"
    val operation = "(\\S+)"
    val key = "(\\S+)"
    val requestURI = "(\\S+ .\\S+ .\\S+)"
    val httpStatus = "(\\S+)"
    val errorCode = "(\\S+)"
    val bytesSent = "(\\S+)"
    val objectSize = "(\\S+)"
    val totalTime = "(\\S+)"
    val turnAroundTime = "(\\S+)"
    val referrer = "(\\S+)"
    val userAgent = "(\\S+)"
    val versionID = "(\\S+)"

    val regex = s"$ownerName $bucketName $dateTime $ip.* $requester $requesterID $operation $key $requestURI $httpStatus $errorCode $bytesSent $objectSize $totalTime $turnAroundTime $referrer $userAgent $versionID"
    Pattern.compile(regex)
  }

  def parseDate(dateTime: String): Date = {
    val dt = new SimpleDateFormat("dd/MMMM/yyyy:HH:mm:ssZ");
    dt.setTimeZone(TimeZone.getTimeZone("GMT"));
    return dt.parse(dateTime)
  }

  def parseLog(line: String): S3LogEntry = {
    val pattern = s3LogPattern()
    val matcher: Matcher = pattern.matcher(line)
    if (matcher.matches) {
      val ownerName = matcher.group(1)
      val bucketName = matcher.group(2)
      val dateTime = util.Try(new java.sql.Date(parseDate(matcher.group(3).substring(2, matcher.group(3).length() - 1)).getTime)) getOrElse (new java.sql.Date(new java.util.Date().getTime))
      val ip = matcher.group(4)
      val requester = matcher.group(5)
      val requesterID = matcher.group(6)
      val operation = matcher.group(7)
      val key = matcher.group(8)
      val requestURI = matcher.group(9)
      val httpStatus = util.Try(matcher.group(10).toInt) getOrElse -999
      val errorCode = util.Try(matcher.group(11).toInt) getOrElse -999
      val bytesSent = util.Try(matcher.group(12).toInt) getOrElse 0
      val objectSize = util.Try(matcher.group(13).toInt) getOrElse 0
      val totalTime = util.Try(matcher.group(14).toInt) getOrElse -999
      val turnAroundTime = util.Try(matcher.group(15).toInt) getOrElse -999
      val referrer = matcher.group(16)
      val userAgent = matcher.group(17)
      val versionID = matcher.group(18)
      return S3LogEntry(ownerName, bucketName, dateTime, ip, requester, requesterID,
        operation, key, requestURI, httpStatus, errorCode, bytesSent, objectSize, totalTime, turnAroundTime, referrer, userAgent, versionID)
    } else {
      return S3LogEntry("error", "error", new java.sql.Date(new java.util.Date().getTime), "error", "error", "error", "error", "error", "error", -999, -999, -999, -999, -999, -999, "error", "error", "error")
    }
  }
}