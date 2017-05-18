package com.s3.logs.entities;

import java.util.Date;

public class S3LogEntry {
	private String bucketOwner;
	private String bucketName;
	private Date time;
	private String remoteIP;
	private String requester;
	private String requestID;
	private String operation;
	private String key;
	private String requestURI;
	private int httpStatus;
	private String errorCode;
	private long bytesSent;
	private long objectSize;
	private long totalTime;
	private long turnAroundTime;
	private String referrer;
	private String userAgent;
	private String versionID;
	
	public S3LogEntry(String bucketOwner, String bucketName, Date dateTime, String remmoteIP, String requester,
			String requestID, String operation, String key, String requestURI, int httpStatus, String errorCode,
			long bytesSent, long objectSize, long totalTime, long turnAroundTime, String referrer, String userAgent,
			String versionID) {
		super();
		this.bucketOwner = bucketOwner;
		this.bucketName = bucketName;
		this.time = dateTime;
		this.remoteIP = remmoteIP;
		this.requester = requester;
		this.requestID = requestID;
		this.operation = operation;
		this.key = key;
		this.requestURI = requestURI;
		this.httpStatus = httpStatus;
		this.errorCode = errorCode;
		this.bytesSent = bytesSent;
		this.objectSize = objectSize;
		this.totalTime = totalTime;
		this.turnAroundTime = turnAroundTime;
		this.referrer = referrer;
		this.userAgent = userAgent;
		this.versionID = versionID;
	}
	
	public String getBucketOwner() {
		return bucketOwner;
	}
	public void setBucketOwner(String bucketOwner) {
		this.bucketOwner = bucketOwner;
	}
	public String getBucketName() {
		return bucketName;
	}
	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}
	public Date getTime() {
		return time;
	}
	public void setTime(Date time) {
		this.time = time;
	}
	public String getIp() {
		return remoteIP;
	}
	public void setIp(String remoteIP) {
		this.remoteIP = remoteIP;
	}
	public String getRequester() {
		return requester;
	}
	public void setRequester(String requester) {
		this.requester = requester;
	}
	public String getRequestID() {
		return requestID;
	}
	public void setRequestID(String requestID) {
		this.requestID = requestID;
	}
	public String getOperation() {
		return operation;
	}
	public void setOperation(String operation) {
		this.operation = operation;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getRequestURI() {
		return requestURI;
	}
	public void setRequestURI(String requestURI) {
		this.requestURI = requestURI;
	}
	public int getHttpStatus() {
		return httpStatus;
	}
	public void setHttpStatus(int httpStatus) {
		this.httpStatus = httpStatus;
	}
	public String getErrorCode() {
		return errorCode;
	}
	public void setErrorCode(String errorCode) {
		this.errorCode = errorCode;
	}
	public long getBytesSent() {
		return bytesSent;
	}
	public void setBytesSent(long bytesSent) {
		this.bytesSent = bytesSent;
	}
	public long getObjectSize() {
		return objectSize;
	}
	public void setObjectSize(long objectSize) {
		this.objectSize = objectSize;
	}
}
