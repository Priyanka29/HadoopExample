/*
  Author: Priyanka29
  Description: Count the number of hits to a file from apache logs.
*/
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NumHitsFileMapper 
  extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final IntWritable one = new IntWritable(1);
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  Pattern pattern = Pattern.compile("(/assets/js/the-associates.js)",Pattern.CASE_INSENSITIVE);
	  Matcher matcher = pattern.matcher(value.toString());
		while (matcher.find()) {
			context.write(new Text(matcher.group(0)),new IntWritable(1) );
		 } 	  
	  
  }

}

    
