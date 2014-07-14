/*
  Author: Priyanka29
  Description: Find the total sales of a particular product.
*/
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StoreExampleMapper 
  extends Mapper<LongWritable, Text, Text, FloatWritable> {

  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] line = value.toString().split("\t");
    
    	  context.write(new  Text(line[3]), new FloatWritable(Float.parseFloat(line[4])));
  }

}

    
