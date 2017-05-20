package com.s3analytics.logs.parser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.s3.logs.entities.S3LogEntry;

/**
 * It parses AWS S3 log files and creates a list s3 entries for further processing.
 * It is currently done based on schema published on @see {a href= "http://docs.aws.amazon.com/AmazonS3/latest/dev/LogFormat.html"} 
 * Parsing is done on the basis of split done on space delimiter. 
 * 
 * @author Nikhil.Bhide
 */
public class S3LogsParser {

	/**
	 * Parses field based on requried formated and returen date object.
	 * 
	 * @param field The input field to be parsed
	 * @return The parsed date
	 * @throws ParseException
	 */
	private Date getDateTime(String field) throws ParseException {
		DateFormat df = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss");
		return df.parse(field);
	}
	
	/**
	 * Create list of S3LogEntries out of s3 log file
	 * @param line The s3 log line
	 * @return The object of {@link S3LogEntry}
	 */
	private S3LogEntry getS3LogEntry(String line) {
		String [] fields = line.split(" ");
		String ownerName = fields[0];
		String bucketName = fields[1];
		Date dateTime = null;
		try {
			dateTime = getDateTime(fields[2].substring(1, fields[2].length()));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String remoteIP = fields[4];
		String requester = fields[5];
		String requestID = fields[6];
		String operation = fields[7];
		String key = fields[8];
		String requestType = fields[9];
		String url = fields[10];
		String httpVersion = fields[11];
		int httpStaus = Integer.parseInt(fields[12]);
		String errorCode = fields[13];
		long bytesSent = getLongField(fields[14]);
		long objectSize = getLongField(fields[15]);
		long totalTime = getLongField(fields[16]);
		long turnAroundTime = getLongField(fields[17]);		
		String referrer = fields[18];
		String userAgent = fields[19];
		String versionID = fields[20];
		
		return new S3LogEntry(ownerName,bucketName,dateTime,remoteIP,requester,requestID,operation,key,requestType.concat(url).concat(httpVersion),httpStaus,errorCode,bytesSent,objectSize,totalTime,turnAroundTime,referrer,userAgent,versionID);
	}
	
	/**
	 * Checks whether input string can be converted to long, returns it otherwise returns default value i.e 0.
	 * @param inputField The input string 
	 * @return The long value
	 */
	private long getLongField(String inputField) {
		try {
		return Long.parseLong(inputField);
		}
		catch(NumberFormatException numberFormatException) {
			return 0;
		}
	}


	/**
	 * Read logs from input file name
	 *  
	 * @param fileName The input file path
	 * @return list The lines from log files 
	 * @throws ParseException 
	 */
	public List<S3LogEntry> getLogs(String filePath) throws ParseException {
		List<S3LogEntry> list = new ArrayList<>();
		
		try (Stream<String> stream = Files.lines(Paths.get(filePath))) {
			list = stream.map(line -> getS3LogEntry(line))
					.collect(Collectors.toList());
		} catch(IOException exc) {
			exc.printStackTrace();
		}

		return list;
	}
}