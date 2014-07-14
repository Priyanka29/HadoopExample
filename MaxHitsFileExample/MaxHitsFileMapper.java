/*
    Arthur : Priyanka29
    Description: Map Reduce Program to Find the Max Hit File From the Apache Log
*/
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxHitsFileMapper 
  extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final IntWritable one = new IntWritable(1);
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  Pattern pattern = Pattern.compile("(\\s/.*\\.\\w{1,3}\\s)",Pattern.CASE_INSENSITIVE);
	  Matcher matcher = pattern.matcher(value.toString());
		while (matcher.find()) {
			System.out.println(matcher.group(0));
			context.write(new Text(matcher.group(0)),new IntWritable(1) );
		 } 	  
	  
  }

}

    
