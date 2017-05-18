package test.s3.logs;

import static org.junit.Assert.*;

import java.text.ParseException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.Test;

import com.s3.logs.entities.S3LogEntry;
import com.s3analytics.logs.parser.S3LogsParser;

public class S3LogsParserTest {
	private S3LogsParser s3LogsParserInstance = new S3LogsParser();
	
	@Test
	public void testLogsParser_CompareCalculatedTotalBytesWithActual_ValuesEqual() throws ParseException {
		List<S3LogEntry> logEntries = s3LogsParserInstance.getLogs("D:\\Work\\Work\\Big Data\\AWS\\Resources\\s3samplelogs.txt");
		Optional<Long> totalBytesSent = logEntries.stream()
		          .map(s3LogEntry->s3LogEntry.getBytesSent())
		          .reduce((x,y)->x + y);
		          
		assertEquals(878,totalBytesSent.get().longValue());
	}
	
	@Test
	public void testLogsParser_GetDistinctBucket_RetrieveSingleBucket() throws ParseException {
		List<S3LogEntry> logEntries = s3LogsParserInstance.getLogs("D:\\Work\\Work\\Big Data\\AWS\\Resources\\s3samplelogs.txt");
		String bucketName = logEntries.stream()
									  .map(record->record.getBucketName())
									  .collect(Collectors.toList())
									  .stream()
									  .collect(Collectors.toList())
									  .get(0);					  								  		  
		assertEquals(bucketName,"mybucket");
	}
}