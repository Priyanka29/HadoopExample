/*
  Author: Priyanka29
  Description: Find the inverted index for a particular word from forum data.
*/
import java.io.IOException;
import javax.swing.text.html.HTMLDocument.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer
  extends Reducer<Text, Text, Text, Text> {
  
  @Override
  public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
	  for(Text value:values)
		  context.write(key, value);
  }

}

