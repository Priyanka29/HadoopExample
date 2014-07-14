/*
  Author: Priyanka29
  Description: Find the inverted index for a particular word from forum data.
*/
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class InvertedIndexMapper 
  extends Mapper<LongWritable, Text, Text, Text> {

  //Pattern pattern = Pattern.compile("(fantastic)",Pattern.CASE_INSENSITIVE);
	Pattern pattern = Pattern.compile("(fantastically)",Pattern.CASE_INSENSITIVE);
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  Matcher matcher = pattern.matcher(value.toString());
		while (matcher.find()) {
			String[] list = value.toString().split("\\s");
		   // context.write(new Text(matcher.group(0)),new IntWritable(1) );
		    context.write(new Text(matcher.group(0)),new Text(list[0]));
		} 
		
		
  }

}

    
